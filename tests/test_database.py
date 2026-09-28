"""
Тесты StateManager и моделей базы данных (database.py).

Покрывают:
1. Инициализацию БД
2. Отметку обработанных файлов
3. Инкрементальную обработку (get_last_line_number)
4. Проверку is_processed
5. Очистку старых записей
6. Статистику парсера
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from database import StateManager, ProcessedFile, ParserStats, init_database


@pytest.fixture
def temp_db(tmp_path):
    """Временная база данных для тестов."""
    db_path = tmp_path / "test_state.db"
    return str(db_path)


@pytest.fixture
def state_manager(temp_db):
    """StateManager с временной БД."""
    return StateManager(temp_db)


@pytest.fixture
def sample_log_file(tmp_path):
    """Пример лог-файла для тестов."""
    log_file = tmp_path / "26092610.log"
    log_file.write_bytes(
        b"00:01.572065-0,SDBL,4,p:processName=test,"
        b"t:computerName=srv,t:connectID=100,Usr=TestUser,"
        b"Func=BeginTransaction\n"
    )
    return log_file


# ============================================================================
# Инициализация
# ============================================================================

class TestStateManagerInit:
    """Тесты инициализации StateManager."""

    def test_creates_db_file(self, temp_db):
        """StateManager создаёт файл БД."""
        sm = StateManager(temp_db)
        assert Path(temp_db).exists()
        sm.close()

    def test_creates_tables(self, state_manager):
        """StateManager создаёт таблицы."""
        with state_manager.get_session() as session:
            result = session.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = [row[0] for row in result]
            assert 'processed_files' in tables
            assert 'parser_stats' in tables


# ============================================================================
# Отметка обработанных файлов
# ============================================================================

class TestMarkProcessed:
    """Тесты отметки обработанных файлов."""

    def test_mark_new_file(self, state_manager, sample_log_file):
        """Отметка нового файла."""
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            last_event_timestamp=datetime(2026, 9, 26, 10, 1, 57),
            is_complete=True
        )

        with state_manager.get_session() as session:
            record = session.query(ProcessedFile).first()
            assert record is not None
            assert record.events_count == 10
            assert record.last_line_number == 100
            assert record.is_complete is True

    def test_mark_file_updates_existing(self, state_manager, sample_log_file):
        """Повторная отметка обновляет существующую запись."""
        # Первая отметка
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=False
        )

        # Вторая отметка (инкрементальная обработка)
        state_manager.mark_processed(
            sample_log_file,
            events_count=5,
            last_line_number=150,
            is_complete=True
        )

        with state_manager.get_session() as session:
            record = session.query(ProcessedFile).first()
            assert record.events_count == 15  # 10 + 5
            assert record.last_line_number == 150
            assert record.is_complete is True


# ============================================================================
# Получение последней обработанной строки
# ============================================================================

class TestGetLastLineNumber:
    """Тесты получения последней обработанной строки."""

    def test_new_file_returns_zero(self, state_manager, sample_log_file):
        """Для нового файла возвращается 0."""
        result = state_manager.get_last_line_number(sample_log_file)
        assert result == 0

    def test_returns_last_line_after_mark(self, state_manager, sample_log_file):
        """После отметки возвращается last_line_number."""
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=False
        )

        result = state_manager.get_last_line_number(sample_log_file)
        assert result == 100

    def test_returns_position_even_if_complete(self, state_manager, sample_log_file):
        """Возвращает позицию даже если файл помечен как завершённый."""
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=True
        )

        result = state_manager.get_last_line_number(sample_log_file)
        assert result == 100


# ============================================================================
# Проверка is_processed
# ============================================================================

class TestIsProcessed:
    """Тесты проверки is_processed."""

    def test_new_file_not_processed(self, state_manager, sample_log_file):
        """Новый файл не помечен как обработанный."""
        result = state_manager.is_processed(sample_log_file)
        assert result is False

    def test_complete_file_is_processed(self, state_manager, sample_log_file):
        """Завершённый файл помечен как обработанный."""
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=True
        )

        result = state_manager.is_processed(sample_log_file)
        assert result is True

    def test_incomplete_file_not_processed(self, state_manager, sample_log_file):
        """Незавершённый файл не помечен как обработанный."""
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=False
        )

        result = state_manager.is_processed(sample_log_file)
        assert result is False

    def test_modified_file_not_processed(self, state_manager, sample_log_file):
        """Изменённый файл не помечен как обработанный."""
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=True
        )

        # Модифицируем файл
        time.sleep(0.1)
        sample_log_file.write_bytes(
            b"00:01.572065-0,SDBL,4,p:processName=test\n"
            b"00:02.123456-100,TLOCK,4,p:processName=test\n"
        )

        result = state_manager.is_processed(sample_log_file)
        assert result is False


# ============================================================================
# Статистика парсера
# ============================================================================

class TestParserStats:
    """Тесты статистики парсера."""

    def test_start_scan(self, state_manager):
        """start_scan создаёт новую запись статистики."""
        stats_id = state_manager.start_scan()
        assert stats_id > 0

    def test_update_scan(self, state_manager):
        """update_scan обновляет статистику."""
        stats_id = state_manager.start_scan()
        state_manager.update_scan(
            stats_id,
            files_scanned=10,
            files_processed=5,
            total_events=100,
            errors_count=2
        )

        with state_manager.get_session() as session:
            stats = session.query(ParserStats).filter(
                ParserStats.id == stats_id
            ).first()
            assert stats.files_scanned == 10
            assert stats.files_processed == 5
            assert stats.total_events == 100
            assert stats.errors_count == 2

    def test_get_stats(self, state_manager):
        """get_stats возвращает общую статистику."""
        stats_id = state_manager.start_scan()
        state_manager.update_scan(stats_id, total_events=100)

        stats = state_manager.get_stats()
        assert 'total_processed' in stats
        assert 'total_events' in stats
        assert 'last_scan' in stats


# ============================================================================
# Очистка старых записей
# ============================================================================

class TestCleanup:
    """Тесты очистки старых записей."""

    def test_cleanup_inactive(self, state_manager, sample_log_file):
        """cleanup_inactive помечает старые записи как неактивные."""
        # Создаём старую запись
        state_manager.mark_processed(
            sample_log_file,
            events_count=10,
            last_line_number=100,
            is_complete=True
        )

        # Имитируем старую запись
        with state_manager.get_session() as session:
            record = session.query(ProcessedFile).first()
            record.processed_at = datetime.utcnow() - timedelta(days=10)

        # Запускаем очистку
        state_manager.cleanup_inactive(days=7)

        # Проверяем
        with state_manager.get_session() as session:
            record = session.query(ProcessedFile).first()
            assert record.is_active is False


# ============================================================================
# Инициализация БД
# ============================================================================

class TestInitDatabase:
    """Тесты инициализации базы данных."""

    def test_init_database(self, tmp_path):
        """init_database создаёт структуру БД."""
        db_path = tmp_path / "init_test.db"
        init_database(str(db_path))

        assert db_path.exists()

        sm = StateManager(str(db_path))
        with sm.get_session() as session:
            result = session.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = [row[0] for row in result]
            assert 'processed_files' in tables
            assert 'parser_stats' in tables
        sm.close()