"""
Интеграционные тесты.

Тестируют взаимодействие компонентов:
- parser.py + database.py (StateManager)
- parser.py + ClickHouse (mock)
"""

import sys
from pathlib import Path
from datetime import datetime

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import parse_file
from database import StateManager
from tests.conftest import MockClickHouseLoader


@pytest.fixture
def temp_db(tmp_path):
    """Временная БД."""
    return str(tmp_path / "test_integration.db")


@pytest.fixture
def state_manager(temp_db):
    """StateManager для интеграционных тестов."""
    return StateManager(temp_db)


@pytest.fixture
def mock_ch_loader():
    """Мок ClickHouseLoader."""
    return MockClickHouseLoader()


# ============================================================================
# Parser + Database
# ============================================================================

class TestParserWithDatabase:
    """Интеграция parser.py + StateManager."""

    def test_incremental_processing(self, state_manager, tmp_path, mock_ch_loader):
        """
        Инкрементальная обработка файла:
        1. Первый проход: парсим файл, сохраняем состояние
        2. Дописываем новые события
        3. Второй проход: парсим только новые события
        """
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test,t:connectID=100\n"
        )

        # Первый проход
        events1, last_line1, last_ts1, is_complete1 = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        state_manager.mark_processed(
            log_file,
            events_count=events1,
            last_line_number=last_line1,
            last_event_timestamp=last_ts1,
            is_complete=is_complete1
        )

        # Дописываем новые события
        with open(log_file, 'ab') as f:
            f.write(b"00:02.123456-100,TLOCK,4,p:processName=test,t:connectID=200\n")

        # Второй проход
        last_line_from_db = state_manager.get_last_line_number(log_file)
        mock_ch_loader2 = MockClickHouseLoader()
        events2, _, _, _ = parse_file(
            log_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line_from_db
        )

        assert events1 == 1
        assert events2 == 1
        events = mock_ch_loader2.get_all_events()
        assert events[0]['event_name'] == 'TLOCK'

    def test_full_processing_cycle(self, state_manager, tmp_path, mock_ch_loader):
        """
        Полный цикл обработки:
        1. Новый файл → парсим полностью
        2. Файл не изменился → пропускаем
        3. Файл изменился → парсим инкрементально
        4. Файл завершён → пропускаем
        """
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
        )

        # 1. Новый файл
        assert state_manager.is_processed(log_file) is False
        events1, last_line1, last_ts1, _ = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )
        state_manager.mark_processed(
            log_file,
            events_count=events1,
            last_line_number=last_line1,
            last_event_timestamp=last_ts1,
            is_complete=False
        )

        # 2. Файл не изменился
        assert state_manager.is_processed(log_file) is False  # Не завершён

        # 3. Файл изменился
        import time
        time.sleep(0.1)
        with open(log_file, 'ab') as f:
            f.write(b"00:02.123456-100,TLOCK,4,p:processName=test\n")

        assert state_manager.is_processed(log_file) is False
        last_line = state_manager.get_last_line_number(log_file)
        mock_ch_loader2 = MockClickHouseLoader()
        events2, last_line2, last_ts2, _ = parse_file(
            log_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line
        )
        state_manager.mark_processed(
            log_file,
            events_count=events2,
            last_line_number=last_line2,
            last_event_timestamp=last_ts2,
            is_complete=True
        )

        # 4. Файл завершён
        assert state_manager.is_processed(log_file) is True


# ============================================================================
# Parser + ClickHouse (mock)
# ============================================================================

class TestParserWithClickHouse:
    """Интеграция parser.py + ClickHouse (mock)."""

    def test_batch_insertion(self, tmp_path, mock_ch_loader):
        """
        Батчевая вставка событий в ClickHouse:
        1. Парсим файл
        2. Разбиваем на батчи
        3. Вставляем в ClickHouse
        """
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
            b"00:02.123456-100,TLOCK,4,p:processName=test\n"
            b"00:03.654321-200,SDBL,4,p:processName=test\n"
        )

        parse_file(log_file, "test_dir", mock_ch_loader,
                   batch_size=2, start_line=0)

        assert mock_ch_loader.total_inserted == 3
        assert len(mock_ch_loader.inserted_batches) == 2  # 2 + 1

    def test_event_fields_mapping(self, tmp_path, mock_ch_loader):
        """
        Проверка маппинга полей события в ClickHouse:
        - timestamp → timestamp
        - event_name → event_name
        - p_processName → p_processName
        - и т.д.
        """
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test_app,"
            b"t:computerName=Server1C,t:connectID=100,Usr=TestUser,"
            b"Func=BeginTransaction\n"
        )

        parse_file(log_file, "test_dir", mock_ch_loader,
                   batch_size=100, start_line=0)

        events = mock_ch_loader.get_all_events()
        assert len(events) == 1

        event = events[0]

        # Проверяем, что все нужные поля есть
        assert 'timestamp' in event
        assert 'event_name' in event
        assert 'p_processName' in event
        assert 't_computerName' in event
        assert 't_connectID' in event
        assert 'usr' in event
        assert 'func' in event

        # Проверяем значения
        assert event['event_name'] == 'SDBL'
        assert event['p_processName'] == 'test_app'
        assert event['t_computerName'] == 'Server1C'
        assert event['t_connectID'] == '100'
        assert event['usr'] == 'TestUser'
        assert event['func'] == 'BeginTransaction'


# ============================================================================
# End-to-End
# ============================================================================

class TestEndToEnd:
    """Сквозные тесты."""

    def test_full_pipeline(self, state_manager, tmp_path, mock_ch_loader):
        """
        Полный пайплайн:
        1. Создаём лог-файл
        2. Парсим инкрементально
        3. Сохраняем состояние
        4. Дописываем новые события
        5. Парсим только новые
        6. Вставляем в ClickHouse
        """
        log_file = tmp_path / "26092610.log"
        log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
        )

        # Первый проход
        events1, last_line1, last_ts1, _ = parse_file(
            log_file, "test_dir", mock_ch_loader,
            batch_size=100, start_line=0
        )

        state_manager.mark_processed(
            log_file,
            events_count=events1,
            last_line_number=last_line1,
            last_event_timestamp=last_ts1,
            is_complete=False
        )

        # Дописываем
        import time
        time.sleep(0.1)
        with open(log_file, 'ab') as f:
            f.write(b"00:02.123456-100,TLOCK,4,p:processName=test\n")

        # Второй проход
        last_line = state_manager.get_last_line_number(log_file)
        mock_ch_loader2 = MockClickHouseLoader()
        events2, last_line2, last_ts2, _ = parse_file(
            log_file, "test_dir", mock_ch_loader2,
            batch_size=100, start_line=last_line
        )

        state_manager.mark_processed(
            log_file,
            events_count=events2,
            last_line_number=last_line2,
            last_event_timestamp=last_ts2,
            is_complete=True
        )

        # Проверяем
        assert mock_ch_loader.total_inserted == 1
        assert mock_ch_loader2.total_inserted == 1
        assert state_manager.is_processed(log_file) is True