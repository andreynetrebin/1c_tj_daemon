"""
Тесты на реальных логах ТЖ 1С.

Используют реальные фрагменты логов из production-среды:
- 26092616.log (CommitTransaction + EXCP)
- 26092617.log (ранее 26092616_real_tail.log)
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import parse_file, EVENT_HEADER_PATTERN
from tests.conftest import MockClickHouseLoader


# ============================================================================
# Тесты на реальном логе 26092616.log
# ============================================================================

class TestRealLog26092616:
    """Тесты на реальном логе 26092616.log (CommitTransaction)."""

    def test_file_exists(self, real_log_26092616):
        """Файл существует."""
        assert real_log_26092616.exists()

    def test_parses_without_errors(self, real_log_26092616, mock_ch_loader):
        """Файл парсится без исключений."""
        events_count, _, _, _ = parse_file(
            real_log_26092616, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count > 0

    def test_majority_events_are_sdbl(self, real_log_26092616, mock_ch_loader):
        """
        Большинство событий в этом файле — SDBL.

        Примечание: в реальном логе есть 1 событие EXCP в конце файла,
        поэтому нельзя требовать, чтобы ВСЕ события были SDBL.
        """
        parse_file(
            real_log_26092616, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()
        sdbl_count = sum(1 for e in events if e['event_name'] == 'SDBL')

        # Большинство (>90%) событий должны быть SDBL
        assert sdbl_count / len(events) > 0.9, (
            f"Ожидалось >90% SDBL, получено {sdbl_count}/{len(events)}"
        )

    def test_has_exc_event(self, real_log_26092616, mock_ch_loader):
        """В реальном логе должно быть хотя бы одно событие EXCP."""
        parse_file(
            real_log_26092616, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()
        excp_events = [e for e in events if e['event_name'] == 'EXCP']
        assert len(excp_events) >= 1, (
            "В реальном логе должно быть хотя бы одно событие EXCP"
        )

    def test_all_events_have_timestamp(self, real_log_26092616, mock_ch_loader):
        """Все события имеют timestamp."""
        parse_file(
            real_log_26092616, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()
        for event in events:
            assert event['timestamp'] is not None, (
                f"Событие {event['event_name']} не имеет timestamp"
            )

    def test_sdbl_events_have_func(self, real_log_26092616, mock_ch_loader):
        """
        SDBL-события имеют корректный func.

        Примечание: из-за дубликатов `Func=Transaction,Func=CommitTransaction`
        парсер сохраняет ПОСЛЕДНЕЕ значение → 'CommitTransaction'.
        Это ожидаемое поведение существующего парсера.
        """
        parse_file(
            real_log_26092616, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()
        sdbl_events = [e for e in events if e['event_name'] == 'SDBL']

        for event in sdbl_events:
            # Функция может быть Transaction или CommitTransaction (последний wins)
            assert event['func'] in ('Transaction', 'CommitTransaction'), (
                f"Ожидался func=Transaction|CommitTransaction, получено {event['func']}"
            )


# ============================================================================
# Тесты на реальном логе с хвостом (26092617.log)
# ============================================================================

class TestRealLog26092616Tail:
    """Тесты на реальном логе с хвостом (26092617.log)."""

    def test_file_exists(self, real_log_26092616_tail):
        """Файл существует."""
        assert real_log_26092616_tail.exists()

    def test_parses_without_errors(self, real_log_26092616_tail, mock_ch_loader):
        """Файл парсится без исключений."""
        events_count, _, _, _ = parse_file(
            real_log_26092616_tail, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        assert events_count > 0

    def test_event_types_present(self, real_log_26092616_tail, mock_ch_loader):
        """Присутствуют ожидаемые типы событий."""
        parse_file(
            real_log_26092616_tail, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()
        event_types = set(e['event_name'] for e in events)

        # В реальном логе должны быть SDBL и TLOCK
        assert 'SDBL' in event_types or 'TLOCK' in event_types

    def test_multiline_context(self, real_log_26092616_tail, mock_ch_loader):
        """Многострочный Context обрабатывается корректно."""
        parse_file(
            real_log_26092616_tail, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()

        # Ищем события с многострочным Context
        multiline_events = [
            e for e in events
            if e.get('context') and '\n' in e['context']
        ]

        # В реальном логе должны быть многострочные Context
        assert len(multiline_events) > 0, (
            "Должны быть события с многострочным Context"
        )

    def test_unicode_in_context(self, real_log_26092616_tail, mock_ch_loader):
        """Unicode символы в Context обрабатываются корректно."""
        parse_file(
            real_log_26092616_tail, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        events = mock_ch_loader.get_all_events()

        # Ищем события с русскими символами в Context
        russian_events = [
            e for e in events
            if e.get('context') and any(
                '\u0400' <= c <= '\u04FF'
                for c in e['context']
            )
        ]

        # В реальном логе должны быть русские символы
        assert len(russian_events) > 0, (
            "Должны быть события с русскими символами в Context"
        )


# ============================================================================
# Тесты производительности на реальных логах
# ============================================================================

class TestRealLogPerformance:
    """Тесты производительности на реальных логах."""

    def test_parse_performance(self, real_log_26092616_tail, mock_ch_loader):
        """Парсинг реального лога выполняется быстро."""
        import time
        start = time.time()
        events_count, _, _, _ = parse_file(
            real_log_26092616_tail, "test_dir", mock_ch_loader,
            batch_size=1000, start_line=0
        )
        elapsed = time.time() - start

        assert events_count > 0
        # Должно выполниться быстрее чем за 10 секунд
        assert elapsed < 10.0, (
            f"Парсинг слишком медленный: {elapsed:.2f}s"
        )
        print(f"\n📊 Реальный лог: {events_count} событий за {elapsed:.2f}s")