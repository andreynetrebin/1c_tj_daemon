"""
Экспериментальные тесты для файла 26092618.log
Цель: исследовать, как существующий parser.py обрабатывает этот конкретный файл,
особенно в контексте проблемы "пропуска первого события".
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import parse_file
from tests.conftest import MockClickHouseLoader


class TestExperimental26092618:
    """Экспериментальные тесты для 26092618.log"""

    def test_file_exists(self, real_log_26092618):
        """Проверка, что файл действительно лежит в папке fixtures"""
        assert real_log_26092618.exists(), (
            f"Файл {real_log_26092618} не найден! "
            "Пожалуйста, положите его в tests/fixtures/"
        )

    def test_parses_without_errors(self, real_log_26092618):
        """Файл должен парситься без исключений и возвращать > 0 событий"""
        mock_loader = MockClickHouseLoader()
        events_count, last_line, last_ts, is_complete = parse_file(
            real_log_26092618, "test_dir", mock_loader, batch_size=100, start_line=0
        )

        assert events_count > 0, "Ожидалось > 0 событий, но парсер вернул 0"
        assert last_line > 0, "Ожидалось last_line > 0"
        assert mock_loader.total_inserted == events_count


    def test_first_event_is_captured(self, real_log_26092618, mock_ch_loader):
        """
        ПЕРВОЕ событие в файле должно иметь line_number == 1.
        Это главный тест на баг "запаздывающей эмиссии".
        """
        parse_file(real_log_26092618, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)

        events = mock_ch_loader.get_all_events()
        assert len(events) > 0, "События не были распарсены"

        first_event = events[0]

        # 🔧 ИСПРАВЛЕНО: Проверяем, что событие начинается с 1-й строки
        assert first_event['line_number'] == 1, (
            f"БАГ: Первое событие начинается на строке {first_event['line_number']}, "
            f"а должно на строке 1."
        )

        # 🔧 ИСПРАВЛЕНО: В реальном файле 26092618.log первым идет TLOCK, а не SDBL
        assert first_event['event_name'] == 'TLOCK'
        assert first_event['usr'] == 'DefUser'


    def test_inspect_first_few_events(self, real_log_26092618, capsys):
        """
        Вывод первых 3 событий в консоль для ручной проверки.
        Помогает нам увидеть, что именно парсер считает "первым" событием.
        """
        mock_loader = MockClickHouseLoader()
        parse_file(real_log_26092618, "test_dir", mock_loader, batch_size=100, start_line=0)

        events = mock_loader.get_all_events()

        print(f"\n{'=' * 60}")
        print(f"🔍 ИНСПЕКЦИЯ: Всего распарсено событий: {len(events)}")
        print(f"{'=' * 60}")

        for i, ev in enumerate(events[:5]):  # Показываем первые 5
            print(f"Событие #{i + 1}:")
            print(f"  line_number : {ev['line_number']}")
            print(f"  timestamp   : {ev['timestamp']}")
            print(f"  event_name  : {ev['event_name']}")
            print(f"  usr         : {ev['usr']}")
            print(f"  process     : {ev['p_processName']}")
            print("-" * 40)

        assert len(events) >= 1, "Не удалось получить ни одного события для инспекции"

    def test_incremental_resume(self, real_log_26092618):
        """
        Проверка инкрементального возобновления.
        Если мы скажем парсеру начать со строки, полученной при первом проходе,
        он должен вернуть 0 новых событий (если файл не менялся).
        """
        mock_loader1 = MockClickHouseLoader()
        events1, last_line1, _, _ = parse_file(
            real_log_26092618, "test_dir", mock_loader1, batch_size=100, start_line=0
        )

        mock_loader2 = MockClickHouseLoader()
        events2, last_line2, _, _ = parse_file(
            real_log_26092618, "test_dir", mock_loader2, batch_size=100, start_line=last_line1
        )

        assert events2 == 0, (
            f"При resume со строки {last_line1} ожидалось 0 событий, "
            f"но получено {events2}. Возможно, last_line сохраняется некорректно."
        )