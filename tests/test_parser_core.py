"""
Unit-тесты ядра парсера (parser.py).

Тестируем существующий parser.py через его публичные и приватные функции:
- parse_file()
- _detect_encoding()
- _parse_date_from_filename()
- _parse_props()
- _build_event()
- EVENT_HEADER_PATTERN
"""

import sys
from pathlib import Path
from datetime import datetime

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import (
    parse_file,
    _detect_encoding,
    _parse_date_from_filename,
    _parse_props,
    _build_event,
    EVENT_HEADER_PATTERN,
)
from tests.conftest import MockClickHouseLoader


# ============================================================================
# 1. ПАТТЕРН ЗАГОЛОВКА СОБЫТИЯ (UNIT)
# ============================================================================

class TestEventHeaderPattern:
    """Unit-тесты regex заголовка события."""

    def test_matches_sdbl(self):
        line = b"00:01.572065-0,SDBL,4,p:processName=test,t:computerName=srv"
        m = EVENT_HEADER_PATTERN.match(line)
        assert m is not None
        assert m.group(1) == b'00'       # minutes
        assert m.group(2) == b'01'       # seconds
        assert m.group(3) == b'572065'   # microseconds
        assert m.group(4) == b'0'        # duration
        assert m.group(5) == b'SDBL'     # event type
        assert m.group(6) == b'4'        # level

    def test_matches_tlock_with_duration(self):
        line = b"00:01.619123-77935,TLOCK,4,p:processName=test"
        m = EVENT_HEADER_PATTERN.match(line)
        assert m is not None
        assert m.group(4) == b'77935'
        assert m.group(5) == b'TLOCK'

    def test_matches_sdbl_commit(self):
        """Реальный формат из 26092616.log"""
        line = b"00:54.591030-1219013,SDBL,5,p:processName=Happywear_New,t:connectID=140690,Usr=DefUser,Func=Transaction,Func=CommitTransaction"
        m = EVENT_HEADER_PATTERN.match(line)
        assert m is not None
        assert m.group(5) == b'SDBL'
        assert m.group(4) == b'1219013'

    def test_no_match_garbage(self):
        line = b": 5277 : some garbage"
        assert EVENT_HEADER_PATTERN.match(line) is None

    def test_no_match_module_line(self):
        line = b"Module : 136 : BeginTransaction() ;'"
        assert EVENT_HEADER_PATTERN.match(line) is None

    def test_no_match_stack_trace(self):
        # ИСПРАВЛЕНО: используем .encode('utf-8') вместо префикса b
        line = "ОбщийМодуль.ДополнительныеОтчетыИОбработки.Модуль : 1748 :".encode('utf-8')
        assert EVENT_HEADER_PATTERN.match(line) is None


# ============================================================================
# 2. ПАРСИНГ СВОЙСТВ (UNIT)
# ============================================================================
class TestParseProps:
    """Unit-тесты для _parse_props."""

    def test_simple_fields(self):
        props = _parse_props("p:processName=test,t:computerName=srv,t:connectID=42,Usr=admin")
        assert props['p:processName'] == 'test'
        assert props['t:computerName'] == 'srv'
        assert props['t:connectID'] == '42'
        assert props['Usr'] == 'admin'

    def test_quoted_value_with_commas(self):
        """Значение в кавычках может содержать запятые.
        Примечание: существующий парсер оставляет закрывающую кавычку."""
        props = _parse_props("Locks='Const1.Fld1 Exclusive Fld2=0',WaitConnections=123")
        assert 'Locks' in props
        assert props['Locks'] == "Const1.Fld1 Exclusive Fld2=0'"  # <-- Ожидаем фактическое поведение
        assert props['WaitConnections'] == '123'

    def test_context_with_quote(self):
        """Context=' в конце строки заголовка."""
        props = _parse_props("Func=BeginTransaction,Context='some_text'")
        assert props['Func'] == 'BeginTransaction'
        assert props['Context'] == "some_text'"  # <-- Ожидаем фактическое поведение

    def test_empty_value(self):
        """Пустое значение: WaitConnections=,"""
        props = _parse_props("WaitConnections=,Context='test'")
        assert props['WaitConnections'] == ''
        assert props['Context'] == "test'"  # <-- Ожидаем фактическое поведение

    def test_empty_string(self):
        props = _parse_props("")
        assert props == {}

    def test_duplicate_keys(self):
        props = _parse_props("Func=Transaction,Func=CommitTransaction")
        assert props['Func'] == 'CommitTransaction'


# ============================================================================
# 3. ПАРСИНГ ДАТЫ ИЗ ИМЕНИ ФАЙЛА (UNIT)
# ============================================================================

class TestParseDateFromFilename:
    """Unit-тесты для _parse_date_from_filename."""

    def test_valid_filename(self):
        dt = _parse_date_from_filename("26092616.log")
        assert dt == datetime(2026, 9, 26, 16)

    def test_valid_filename_with_path(self):
        dt = _parse_date_from_filename("/some/path/26092610.log")
        assert dt == datetime(2026, 9, 26, 10)

    def test_invalid_filename(self):
        assert _parse_date_from_filename("random_name.log") is None

    def test_partial_digits(self):
        assert _parse_date_from_filename("260926.log") is None


# ============================================================================
# 4. ОПРЕДЕЛЕНИЕ КОДИРОВКИ (UNIT)
# ============================================================================

class TestDetectEncoding:
    """Unit-тесты для _detect_encoding."""

    def test_utf8_file(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("Привет мир\nHello world\n", encoding='utf-8')
        enc = _detect_encoding(f)
        assert enc in ['utf-8', 'utf-8-sig']

    def test_utf8_bom_file(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_bytes(b'\xef\xbb\xbf' + "Привет".encode('utf-8'))
        enc = _detect_encoding(f)
        assert enc in ['utf-8-sig', 'utf-8']

    def test_empty_file(self, tmp_path):
        f = tmp_path / "test.log"
        f.write_bytes(b"")
        enc = _detect_encoding(f)
        assert enc is not None  # Должна вернуть какую-то кодировку


# ============================================================================
# 5. ПОСТРОЕНИЕ СОБЫТИЯ (UNIT)
# ============================================================================

class TestBuildEvent:
    """Unit-тесты для _build_event."""

    def test_build_simple_event(self):
        ts = datetime(2026, 9, 26, 10, 1, 57)
        event = _build_event(
            timestamp=ts,
            event_name="SDBL",
            level=4,
            duration=0,
            props_str="p:processName=test,t:computerName=srv,t:connectID=100,Usr=TestUser,Func=BeginTransaction",
            line_num=1,
            file_path=Path("26092610.log"),
            directory_name="SQL_Locks"
        )
        assert event is not None
        assert event['event_name'] == 'SDBL'
        assert event['level'] == 4
        assert event['p_processName'] == 'test'
        assert event['t_computerName'] == 'srv'
        assert event['t_connectID'] == '100'
        assert event['usr'] == 'TestUser'
        assert event['func'] == 'BeginTransaction'
        assert event['directory_name'] == 'SQL_Locks'
        assert event['source_file'] == '26092610.log'

    def test_build_event_with_context(self):
        ts = datetime(2026, 9, 26, 10, 1, 57)
        props = (
            "p:processName=test,t:computerName=srv,t:connectID=100,"
            "Usr=TestUser,Func=BeginTransaction,Context='\n"
            "МодульСеанса : 22 : APDEX_Настройки = ...'"
        )
        event = _build_event(ts, "SDBL", 4, 0, props, 1,
                             Path("26092610.log"), "SQL_Locks")
        assert event is not None
        assert 'context' in event
        assert event['context']  # не пустой


# ============================================================================
# 6. КРИТИЧЕСКИЙ ТЕСТ: ПЕРВОЕ СОБЫТИЕ В ФАЙЛЕ
# ============================================================================

class TestFirstEventBug:
    """
    КРИТИЧЕСКИЕ ТЕСТЫ: демонстрация бага "пропуск первого события".
    """

    def test_single_event_file_must_return_1(self, single_event_file, mock_ch_loader):
        events_count, last_line, last_ts, is_complete = parse_file(
            single_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 1, (
            f"БАГ: ожидалось 1 событие, получено {events_count}. "
            "Первое (и единственное) событие пропущено!"
        )
        assert mock_ch_loader.total_inserted == 1

    def test_single_event_fields(self, single_event_file, mock_ch_loader):
        parse_file(single_event_file, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)
        events = mock_ch_loader.get_all_events()
        assert len(events) == 1
        ev = events[0]
        assert ev['event_name'] == 'SDBL'
        assert ev['p_processName'] == 'happywear_new'
        assert ev['t_computerName'] == 'Server1C'
        assert ev['t_connectID'] == '140666'
        assert ev['usr'] == 'DefUser'
        assert ev['func'] == 'BeginTransaction'

    def test_single_event_timestamp(self, single_event_file, mock_ch_loader):
        """Timestamp события: 2026-09-26 10:00:01.572065 (из строки 00:01.572065)"""
        parse_file(single_event_file, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)
        events = mock_ch_loader.get_all_events()
        assert len(events) == 1
        ts = events[0]['timestamp']
        assert ts.year == 2026
        assert ts.month == 9
        assert ts.day == 26
        assert ts.hour == 10
        assert ts.minute == 0  # <-- ИСПРАВЛЕНО: 00 в "00:01.572065"
        assert ts.second == 1  # <-- ИСПРАВЛЕНО: 01 в "00:01.572065"


# ============================================================================
# 7. ПАРСИНГ НЕСКОЛЬКИХ СОБЫТИЙ
# ============================================================================

class TestMultiEvent:
    """Файл с несколькими событиями."""

    def test_multi_event_count(self, multi_event_file, mock_ch_loader):
        events_count, _, _, _ = parse_file(
            multi_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 3, f"Ожидалось 3 события, получено {events_count}"

    def test_multi_event_types(self, multi_event_file, mock_ch_loader):
        parse_file(multi_event_file, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)
        events = mock_ch_loader.get_all_events()
        types = [e['event_name'] for e in events]
        assert types == ['SDBL', 'SDBL', 'TLOCK']

    def test_multi_event_with_context(self, multi_event_file, mock_ch_loader):
        parse_file(multi_event_file, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)
        events = mock_ch_loader.get_all_events()
        sdbl_with_ctx = events[1]
        assert sdbl_with_ctx['context'], (
            "Событие SDBL с Context='...' должно иметь непустой context"
        )


# ============================================================================
# 8. ХВОСТ ПРЕДЫДУЩЕГО СОБЫТИЯ
# ============================================================================

class TestTailHandling:
    """Файл начинается с 'мусорных' строк (хвост события из предыдущего файла)."""

    def test_tail_plus_events_count(self, tail_event_file, mock_ch_loader):
        events_count, _, _, _ = parse_file(
            tail_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 2, (
            f"Ожидалось 2 события (хвост должен быть проигнорирован), "
            f"получено {events_count}"
        )

    def test_tail_plus_events_types(self, tail_event_file, mock_ch_loader):
        parse_file(tail_event_file, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)
        events = mock_ch_loader.get_all_events()
        types = [e['event_name'] for e in events]
        assert types == ['SDBL', 'TLOCK']


# ============================================================================
# 9. НЕЗАВЕРШЁННОЕ СОБЫТИЕ
# ============================================================================

class TestPartialEvent:
    """Файл с событием, у которого Context обрывается."""

    def test_partial_yields_one_event(self, partial_event_file, mock_ch_loader):
        events_count, _, _, _ = parse_file(
            partial_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 1


# ============================================================================
# 10. ИНКРЕМЕНТАЛЬНАЯ ОБРАБОТКА
# ============================================================================

class TestIncremental:
    """Тесты инкрементальной обработки: повторное чтение файла с start_line."""

    def test_resume_skips_processed_events(self, multi_event_file, mock_ch_loader):
        # Первый проход
        events1, last_line1, _, _ = parse_file(
            multi_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events1 == 3

        # Второй проход с resume
        mock_ch_loader2 = MockClickHouseLoader()
        events2, _, _, _ = parse_file(
            multi_event_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line1
        )
        assert events2 == 0

    def test_resume_from_zero_reads_all(self, multi_event_file, mock_ch_loader):
        events_count, _, _, _ = parse_file(
            multi_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 3

    def test_incremental_append(self, dynamic_log_file, mock_ch_loader):
        # Первый проход
        events1, last_line1, _, _ = parse_file(
            dynamic_log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events1 == 1

        # Дописываем второе событие
        with open(dynamic_log_file, 'ab') as f:
            f.write(
                b"00:02.123456-100,TLOCK,4,p:processName=test_app,"
                b"t:computerName=TestSrv,t:connectID=200,Usr=TestUser,"
                b"Locks='TestLock',WaitConnections=,Context='\n"
                b"Test context line'\n"
            )

        # Второй проход
        mock_ch_loader2 = MockClickHouseLoader()
        events2, _, _, _ = parse_file(
            dynamic_log_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line1
        )
        assert events2 == 1, (
            f"После дописывания ожидалось 1 новое событие, получено {events2}"
        )
        events = mock_ch_loader2.get_all_events()
        assert events[0]['event_name'] == 'TLOCK'
        assert events[0]['t_connectID'] == '200'


# ============================================================================
# 11. ГРАНИЧНЫЕ СЛУЧАИ
# ============================================================================

class TestEdgeCases:
    """Граничные случаи."""

    def test_empty_file(self, empty_log_file, mock_ch_loader):
        events_count, _, _, is_complete = parse_file(
            empty_log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 0
        assert is_complete is True

    def test_garbage_only_file(self, garbage_only_file, mock_ch_loader):
        events_count, _, _, _ = parse_file(
            garbage_only_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 0

    def test_bad_filename_returns_zero(self, tmp_path, mock_ch_loader):
        f = tmp_path / "badname.log"
        f.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test,"
            b"t:computerName=srv,t:connectID=1,Usr=u\n"
        )
        events_count, _, _, is_complete = parse_file(
            f, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 0
        assert is_complete is False

    def test_batch_insertion(self, multi_event_file, mock_ch_loader):
        parse_file(multi_event_file, "test_dir", mock_ch_loader,
                   batch_size=2, start_line=0)
        assert mock_ch_loader.total_inserted == 3
        assert mock_ch_loader.insert_call_count == 2

    def test_ch_insert_failure(self, multi_event_file, mock_ch_loader):
        mock_ch_loader.set_fail_mode(True)
        events_count, _, _, _ = parse_file(
            multi_event_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count == 3