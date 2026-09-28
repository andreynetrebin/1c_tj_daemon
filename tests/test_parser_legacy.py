"""
Тесты существующего парсера (parser.py) — фокус на багах и edge cases.

Эти тесты НЕ модифицируют parser.py, только тестируют его поведение.
Цель: задокументировать известные проблемы и проверить их исправление.
"""

import sys
from pathlib import Path
from datetime import datetime

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import parse_file, _detect_encoding, _parse_date_from_filename, _parse_props
from tests.conftest import MockClickHouseLoader


# ============================================================================
# ДЕмонстрация бага "пропуск первого события"
# ============================================================================

class TestLegacyParserBugs:
    """
    Тесты, демонстрирующие баги существующего парсера.

    КРИТИЧЕСКИЙ БАГ: пропуск первого события в файле.
    """

    def test_single_event_bug(self, tmp_path, mock_ch_loader):
        """
        БАГ: Файл с одним событием возвращает 0 событий.

        Это основной баг, который нужно исправить.
        Тест ДОЛЖЕН ПАДАТЬ, пока баг не исправлен.
        """
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test,"
            b"t:computerName=srv,t:connectID=100,Usr=TestUser,"
            b"Func=BeginTransaction\n"
        )

        events_count, last_line, last_ts, is_complete = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        # ОЖИДАЕМОЕ ПОВЕДЕНИЕ: 1 событие
        # РЕАЛЬНОЕ ПОВЕДЕНИЕ (с багом): 0 событий
        assert events_count == 1, (
            f"БАГ: ожидалось 1 событие, получено {events_count}. "
            "Первое событие пропущено!"
        )

    def test_multi_event_first_skipped(self, tmp_path, mock_ch_loader):
        """В файле с несколькими событиями все должны быть обработаны."""
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test,"
            b"t:computerName=srv,t:connectID=100,Usr=TestUser,"
            b"Func=BeginTransaction\n"
            b"00:02.123456-100,TLOCK,4,p:processName=test,"
            b"t:computerName=srv,t:connectID=200,Usr=TestUser,"
            b"Locks='TestLock'\n"
        )

        events_count, _, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        assert events_count == 2, (
            f"Ожидалось 2 события, получено {events_count}"
        )


# ============================================================================
# Инкрементальная обработка
# ============================================================================

class TestLegacyParserIncremental:
    """Тесты инкрементальной обработки старого парсера."""

    def test_resume_from_start_line(self, tmp_path, mock_ch_loader):
        """Resume с start_line пропускает уже обработанные строки."""
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
            b"00:02.123456-100,TLOCK,4,p:processName=test\n"
            b"00:03.654321-200,SDBL,4,p:processName=test\n"
        )

        # Первый проход
        events1, last_line1, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        # Второй проход с resume
        mock_ch_loader2 = MockClickHouseLoader()
        events2, last_line2, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line1
        )

        # Второй проход должен вернуть 0 событий
        assert events2 == 0

    def test_incremental_append(self, tmp_path, mock_ch_loader):
        """Инкрементальная обработка при дописывании файла."""
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
        )

        # Первый проход
        events1, last_line1, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        # Дописываем второе событие
        with open(log_file, 'ab') as f:
            f.write(b"00:02.123456-100,TLOCK,4,p:processName=test\n")

        # Второй проход с resume
        mock_ch_loader2 = MockClickHouseLoader()
        events2, _, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line1
        )

        # Второй проход должен вернуть 1 новое событие
        assert events2 == 1


# ============================================================================
# Граничные случаи
# ============================================================================

class TestLegacyParserEdgeCases:
    """Тесты граничных случаев старого парсера."""

    def test_empty_file(self, tmp_path, mock_ch_loader):
        """Пустой файл."""
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(b"")

        events_count, _, _, is_complete = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        assert events_count == 0
        assert is_complete is True  # Пустой файл считается завершённым

    def test_garbage_only_file(self, tmp_path, mock_ch_loader):
        """Файл только с мусором (без заголовков)."""
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b": 5277 : some garbage line\n"
            b": 1 : another garbage line\n"
        )

        events_count, _, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        assert events_count == 0

    def test_invalid_filename(self, tmp_path, mock_ch_loader):
        """Файл с некорректным именем (нет даты)."""
        log_file = tmp_path / "invalid_name.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
        )

        events_count, _, _, is_complete = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        assert events_count == 0
        assert is_complete is False