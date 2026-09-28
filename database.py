# /opt/1c_tj_daemon/database.py
"""SQLite модели и менеджер состояния для парсера с инкрементальной обработкой"""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import contextmanager

from sqlalchemy import create_engine, Column, Integer, String, DateTime, BigInteger, Boolean, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import func, and_

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))



from config import settings

logger = logging.getLogger(__name__)
Base = declarative_base()


# ============================================================================
# 🔧 SQLITE МОДЕЛИ
# ============================================================================

class ProcessedFile(Base):
    """Таблица обработанных файлов с инкрементальным трекингом"""
    __tablename__ = 'processed_files'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_hash = Column(String(64), unique=True, nullable=False, index=True)
    file_path = Column(String(1024), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    file_mtime = Column(Float, nullable=False)
    
    # 🔧 НОВЫЕ ПОЛЯ для инкрементальной обработки
    last_line_number = Column(BigInteger, default=0, nullable=False)
    last_event_timestamp = Column(DateTime, nullable=True)
    is_complete = Column(Boolean, default=False, nullable=False)

    # 🔧 ДОПОЛНИТЕЛЬНО: счётчик циклов без изменений
    unchanged_cycles = Column(Integer, default=0, nullable=False)

    events_count = Column(Integer, default=0)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    
    def __repr__(self):
        return f"<ProcessedFile(hash={self.file_hash[:16]}, lines={self.last_line_number}, complete={self.is_complete})>"


class ParserStats(Base):
    """Таблица статистики парсера"""
    __tablename__ = 'parser_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    scan_started_at = Column(DateTime, nullable=False)
    scan_completed_at = Column(DateTime)
    files_scanned = Column(Integer, default=0)
    files_processed = Column(Integer, default=0)
    total_events = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)
    error_message = Column(Text)
    
    def __repr__(self):
        return f"<ParserStats(id={self.id}, events={self.total_events})>"


# ============================================================================
# 🔧 STATE MANAGER
# ============================================================================

class StateManager:
    """Менеджер состояния с поддержкой инкрементальной обработки"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            connect_args={
                "check_same_thread": False,
                "timeout": 30
            },
            pool_pre_ping=True
        )
        
        Base.metadata.create_all(self.engine)
        
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        logger.info(f"✅ StateManager инициализирован: {self.db_path}")
    
    @contextmanager
    def get_session(self) -> Session:
        """Контекстный менеджер для сессии БД"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def _file_hash(self, file_path: Path, size: int, mtime: float) -> str:
        """Вычислить хеш файла (путь + размер + mtime + ctime)"""
        try:
            stat = file_path.stat()
            content = f"{file_path.resolve()}:{size}:{mtime}:{stat.st_ctime}"
            return hashlib.sha256(content.encode()).hexdigest()
        except:
            return hashlib.sha256(str(file_path).encode()).hexdigest()

    # database.py — метод is_processed()


    def is_processed(self, file_path: Path) -> bool:
        """
        Проверить, нужно ли обрабатывать файл.

        Returns:
            False — файл нужно обработать (новый, модифицирован, или не завершён)
            True — файл полностью обработан и не изменялся
        """
        try:
            stat = file_path.stat()
            file_hash = self._file_hash(file_path, stat.st_size, stat.st_mtime)

            with self.get_session() as session:
                record = session.query(ProcessedFile).filter(
                    ProcessedFile.file_hash == file_hash,
                    ProcessedFile.is_active == True
                ).first()

                #  Файл не найден по хешу — нужно обработать
                if not record:
                    logger.debug(f"📄 Новый файл (или изменён): {file_path.name}")
                    return False

                # 🔧 Файл помечен как завершённый И не изменялся — пропускаем
                if record.is_complete and stat.st_mtime == record.file_mtime:
                    logger.debug(f"✅ Файл завершён и не изменялся: {file_path.name}")
                    return True

                # 🔧 Файл модифицирован (mtime изменился) — нужно продолжить
                if stat.st_mtime != record.file_mtime:
                    logger.debug(f"🔄 Файл изменён: {file_path.name} (строка {record.last_line_number})")
                    return False

                # 🔧 Файл не завершён — нужно продолжить обработку
                if not record.is_complete:
                    logger.debug(f" Файл в обработке: {file_path.name} (строка {record.last_line_number})")
                    return False

                # 🔧 Файл не изменялся с последней проверки
                logger.debug(f"⏸ Файл без изменений: {file_path.name}")
                return True

        except Exception as e:
            logger.warning(f"⚠️ Ошибка проверки файла {file_path}: {e}")
            return False

    # database.py — метод get_last_line_number()

    def get_last_line_number(self, file_path: Path) -> int:
        """
        Получить номер последней обработанной строки.
        Важно: Возвращаем позицию даже если файл помечен как завершённый!
        """
        try:
            stat = file_path.stat()
            file_hash = self._file_hash(file_path, stat.st_size, stat.st_mtime)

            with self.get_session() as session:
                # 🔧 Ищем по хешу (точное совпадение файла)
                record = session.query(ProcessedFile).filter(
                    ProcessedFile.file_hash == file_hash,
                    ProcessedFile.is_active == True
                ).first()

                if record:
                    logger.debug(
                        f"📍 {file_path.name}: last_line={record.last_line_number}, complete={record.is_complete}")
                    return record.last_line_number or 0

                # 🔧 Если хеш не найден, ищем по пути (файл мог измениться)
                record_by_path = session.query(ProcessedFile).filter(
                    ProcessedFile.file_path == str(file_path.resolve()),
                    ProcessedFile.is_active == True
                ).order_by(ProcessedFile.processed_at.desc()).first()

                if record_by_path:
                    logger.debug(f" {file_path.name}: найдено по пути, last_line={record_by_path.last_line_number}")
                    return record_by_path.last_line_number or 0

                logger.debug(f"📄 {file_path.name}: новая обработка")
                return 0

        except Exception as e:
            logger.warning(f"️ Ошибка получения позиции {file_path}: {e}")
            return 0
    
    def mark_processed(self, file_path: Path, events_count: int, 
                       last_line_number: int = 0, last_event_timestamp: datetime = None,
                       is_complete: bool = False):
        """
        Отметить файл как обработанный (полностью или частично).
        """
        try:
            stat = file_path.stat()
            file_hash = self._file_hash(file_path, stat.st_size, stat.st_mtime)
            
            with self.get_session() as session:
                existing = session.query(ProcessedFile).filter(
                    ProcessedFile.file_hash == file_hash
                ).first()
                
                if existing:
                    # 🔧 Обновляем существующую запись
                    existing.last_line_number = last_line_number
                    existing.last_event_timestamp = last_event_timestamp
                    existing.is_complete = is_complete
                    existing.events_count = (existing.events_count or 0) + events_count
                    existing.processed_at = datetime.utcnow()
                    existing.is_active = True
                    existing.file_size = stat.st_size
                    existing.file_mtime = stat.st_mtime
                else:
                    # 🔧 Создаём новую запись
                    record = ProcessedFile(
                        file_hash=file_hash,
                        file_path=str(file_path.resolve()),
                        file_size=stat.st_size,
                        file_mtime=stat.st_mtime,
                        events_count=events_count,
                        last_line_number=last_line_number,
                        last_event_timestamp=last_event_timestamp,
                        is_complete=is_complete,
                        processed_at=datetime.utcnow(),
                        is_active=True
                    )
                    session.add(record)
                
                # 🔧 Очистка старых записей
                total = session.query(ProcessedFile).count()
                if total > 50000:
                    old_records = session.query(ProcessedFile).filter(
                        ProcessedFile.is_active == False
                    ).order_by(
                        ProcessedFile.processed_at.asc()
                    ).limit(total - 45000).all()
                    
                    for rec in old_records:
                        session.delete(rec)
                    
                    logger.info(f"🧹 Очищено {len(old_records)} старых записей")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка отметки файла {file_path}: {e}")
            raise
    
    def start_scan(self) -> int:
        """Начать новый цикл сканирования"""
        with self.get_session() as session:
            stats = ParserStats(
                scan_started_at=datetime.utcnow(),
                files_scanned=0,
                files_processed=0,
                total_events=0,
                errors_count=0
            )
            session.add(stats)
            session.flush()
            logger.info(f"📝 Created new scan stats: id={stats.id}")
            return stats.id
    
    def update_scan(self, stats_id: int, files_scanned: int = None, 
                   files_processed: int = None, total_events: int = None,
                   errors_count: int = None, error_message: str = None):
        """Обновить статистику сканирования"""
        with self.get_session() as session:
            stats = session.query(ParserStats).filter(
                ParserStats.id == stats_id
            ).first()
            
            if stats:
                if files_scanned is not None:
                    stats.files_scanned = files_scanned
                if files_processed is not None:
                    stats.files_processed = files_processed
                if total_events is not None:
                    stats.total_events = total_events
                if errors_count is not None:
                    stats.errors_count = errors_count
                if error_message is not None:
                    stats.error_message = error_message
                # 🔧 Всегда устанавливаем время завершения
                if stats.scan_completed_at is None:
                    stats.scan_completed_at = datetime.utcnow()
    
    def get_stats(self) -> Dict[str, Any]:
        """Получить общую статистику"""
        with self.get_session() as session:
            total_processed = session.query(ProcessedFile).filter(
                ProcessedFile.is_active == True
            ).count()
            
            total_events = session.query(ProcessedFile).filter(
                ProcessedFile.is_active == True
            ).with_entities(
                func.sum(ProcessedFile.events_count)
            ).scalar() or 0
            
            last_scan = session.query(ParserStats).order_by(
                ParserStats.scan_started_at.desc()
            ).first()
            
            return {
                'total_processed': total_processed,
                'total_events': total_events,
                'last_scan': last_scan.scan_started_at.isoformat() if last_scan else None,
                'last_scan_completed': last_scan.scan_completed_at.isoformat() if last_scan and last_scan.scan_completed_at else None
            }
    
    def cleanup_inactive(self, days: int = 7):
        """Пометить старые записи как неактивные"""
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        with self.get_session() as session:
            updated = session.query(ProcessedFile).filter(
                and_(
                    ProcessedFile.is_active == True,
                    ProcessedFile.processed_at < cutoff
                )
            ).update({ProcessedFile.is_active: False})
            
            if updated > 0:
                logger.info(f"🧹 Помечено {updated} записей как неактивные")
    
    def close(self):
        """Закрыть соединения"""
        self.engine.dispose()
        logger.info("🔌 StateManager закрыт")


# ============================================================================
# 🔧 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def init_database(db_path: str):
    """Инициализировать базу данных"""
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    engine.dispose()
    logger.info(f"✅ База данных инициализирована: {db_path}")
