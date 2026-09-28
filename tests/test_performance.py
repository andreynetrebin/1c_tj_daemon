"""
Тесты производительности парсера.

Цель: убедиться, что парсер работает быстро на больших файлах.
"""

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import parse_file
from tests.conftest import MockClickHouseLoader


@pytest.fixture
def large_log_file(tmp_path):
    """Большой лог-файл для тестов производительности."""
    log_file = tmp_path / "26092610_large.log"

    # Генерируем 10000 событий
    with open(log_file, 'wb') as f:
        for i in range(10000):
            minute = i // 60
            second = i % 60
            f.write(
                f"{minute:02d}:{second:02d}.123456-0,SDBL,4,"
                f"p:processName=test_app,t:computerName=TestSrv,"
                f"t:connectID={1000 + i},Usr=TestUser,"
                f"Func=BeginTransaction\n".encode('utf-8')
            )

    return log_file


class TestParserPerformance:
    """Тесты производительности парсера."""

    def test_parse_speed(self, large_log_file, mock_ch_loader):
        """parse_file работает быстро на больших файлах."""
        start = time.time()
        events_count, _, _, _ = parse_file(
            large_log_file, "test_dir", mock_ch_loader,
            batch_size=1000, start_line=0
        )
        elapsed = time.time() - start

        assert events_count == 10000
        # Должно выполниться быстрее чем за 10 секунд
        assert elapsed < 10.0, (
            f"parse_file слишком медленный: {elapsed:.2f}s"
        )

        events_per_second = events_count / elapsed
        print(f"\n📊 Скорость парсинга: {events_per_second:.0f} событий/сек")

    def test_batch_insert_speed(self, large_log_file):
        """Батчевая обработка работает быстро."""
        mock_loader = MockClickHouseLoader()
        batch_size = 1000

        start = time.time()
        events_count, _, _, _ = parse_file(
            large_log_file, "test_dir", mock_loader,
            batch_size=batch_size, start_line=0
        )
        elapsed = time.time() - start

        assert events_count == 10000
        assert mock_loader.insert_call_count == 10  # 10000 / 1000
        assert elapsed < 10.0, (
            f"Батчевая обработка слишком медленная: {elapsed:.2f}s"
        )

        print(f"\n📊 Батчей: {mock_loader.insert_call_count}, время: {elapsed:.2f}s")


class TestIncrementalPerformance:
    """Тесты производительности инкрементальной обработки."""

    def test_incremental_speed(self, large_log_file, mock_ch_loader):
        """Инкрементальная обработка работает быстро."""
        # Первый проход
        start = time.time()
        events1, last_line1, _, _ = parse_file(
            large_log_file, "test_dir", mock_ch_loader,
            batch_size=1000, start_line=0
        )
        elapsed1 = time.time() - start

        # Второй проход (resume)
        mock_ch_loader2 = MockClickHouseLoader()
        start = time.time()
        events2, _, _, _ = parse_file(
            large_log_file, "test_dir", mock_ch_loader2,
            batch_size=1000, start_line=last_line1
        )
        elapsed2 = time.time() - start

        assert events1 == 10000
        assert events2 == 0

        print(f"\n📊 Первый проход: {elapsed1:.2f}s")
        print(f"📊 Второй проход: {elapsed2:.2f}s")

        # Второй проход должен быть быстрее
        assert elapsed2 < elapsed1