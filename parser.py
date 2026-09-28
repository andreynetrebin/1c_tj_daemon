#!/opt/1c_tj_daemon/.venv/bin/python3
"""
Фоновый парсер технологических журналов 1С
Версия: 3.0 (Инкрементальная обработка файлов)
"""

import os
import time
import signal
import logging
import re
import gc
import chardet
import clickhouse_connect
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Generator, Tuple


import sys

# Динамически определяем корневую директорию проекта (там, где лежит parser.py)
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from database import StateManager, init_database


# ============================================================================
# 🔧 НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================================

def setup_logging():
    """Настройка логирования"""
    log_file = Path(settings.parser_log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, settings.parser_log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()

# ============================================================================
# 🔧 КОНСТАНТЫ И ПАТТЕРНЫ
# ============================================================================

UTF8_BOM = b'\xef\xbb\xbf'
POSSIBLE_ENCODINGS = ['utf-8-sig', 'utf-8', 'cp1251', 'cp866']

EVENT_HEADER_PATTERN = re.compile(
    rb'^(\d{2}):(\d{2})\.(\d+)-(\d+),'
    rb'\s*([^,]+),'
    rb'\s*(\d+),'
    rb'\s*(.*)$'
)


# ============================================================================
# 🔧 CLICKHOUSE LOADER
# ============================================================================

class ClickHouseLoader:
    """Загрузчик событий в ClickHouse"""

    def __init__(self, config: dict, table_name: str):
        self.config = config
        self.table_name = table_name
        self.client = None
        self._connect()

    def _connect(self):
        """Подключиться к ClickHouse"""
        logger.info(f"🔌 Подключение к ClickHouse: {self.config['host']}:{self.config['port']}")
        self.client = clickhouse_connect.get_client(
            host=self.config['host'],
            port=int(self.config['port']),
            username=self.config['username'],
            password=self.config['password'],
            database=self.config['database'],
            compress=True,
            send_receive_timeout=300
        )
        self._ensure_table()
        logger.info(f"✅ ClickHouse подключён, таблица: {self.table_name}")

    def _ensure_table(self):
        """Создать таблицу если не существует"""
        quoted = f"`{self.table_name}`"
        db = self.config['database']

        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {db}.{quoted}
            (
                timestamp DateTime64(6, 'Europe/Moscow'),
                directory_name LowCardinality(String),
                source_file String,
                line_number UInt64,
                loaded_at DateTime64(6, 'Europe/Moscow'),
                event_name LowCardinality(String),
                level UInt8,
                duration_ms Float32,
                p_processName LowCardinality(String),
                t_computerName String,
                t_connectID String,
                usr LowCardinality(String),
                dbpid String,
                osThread String,
                sessionID String,
                trans String,
                func String,
                tableName LowCardinality(String),
                context String CODEC(ZSTD(3)),
                sql String CODEC(ZSTD(3)),
                sdbl String CODEC(ZSTD(3)),
                planSQLText String CODEC(ZSTD(3)),
                exception String CODEC(ZSTD(3)),
                locks String,
                waitConnections String,
                deadlockConnectionIntersections String,
                lkaid String,
                lka String,
                lkp String,
                lkpid String,
                lksrc String,
                rows String,
                rowsAffected String,
                description String,
                data String CODEC(ZSTD(3)),
                severity LowCardinality(String),
                category LowCardinality(String),
                mssql_error_code Nullable(UInt32),
                INDEX idx_ts timestamp TYPE minmax GRANULARITY 8192,
                INDEX idx_en event_name TYPE bloom_filter GRANULARITY 8192,
                INDEX idx_usr usr TYPE bloom_filter GRANULARITY 8192,
                INDEX idx_err mssql_error_code TYPE set(100) GRANULARITY 8192
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (timestamp, event_name)
            TTL timestamp + INTERVAL 90 DAY
            SETTINGS index_granularity = 8192
        """)
        logger.info(f"✅ Таблица {db}.{quoted} готова")

    def insert_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Вставить батч событий"""
        if not events:
            return True
        try:
            rows = []
            for e in events:
                rows.append([
                    e.get('timestamp'),
                    e.get('directory_name', ''),
                    e.get('source_file', ''),
                    int(e.get('line_number', 0) or 0),
                    datetime.now(),
                    e.get('event_name', ''),
                    int(e.get('level', 0) or 0),
                    float(e.get('duration_ms', 0) or 0),
                    e.get('p_processName', ''),
                    e.get('t_computerName', ''),
                    e.get('t_connectID', ''),
                    e.get('usr', ''),
                    e.get('dbpid', ''),
                    e.get('osThread', ''),
                    e.get('sessionID', ''),
                    e.get('trans', ''),
                    e.get('func', ''),
                    e.get('tableName', ''),
                    (e.get('context') or '')[:100000],
                    (e.get('sql') or '')[:100000],
                    (e.get('sdbl') or '')[:100000],
                    (e.get('planSQLText') or '')[:100000],
                    (e.get('exception') or '')[:100000],
                    e.get('locks', ''),
                    e.get('waitConnections', ''),
                    e.get('deadlockConnectionIntersections', ''),
                    e.get('lkaid', ''),
                    e.get('lka', ''),
                    e.get('lkp', ''),
                    e.get('lkpid', ''),
                    e.get('lksrc', ''),
                    e.get('rows', ''),
                    e.get('rowsAffected', ''),
                    e.get('description', ''),
                    (e.get('data') or '')[:100000],
                    e.get('severity', 'info'),
                    e.get('category', 'other'),
                    e.get('mssql_error_code')
                ])

            self.client.insert(
                table=f"`{self.table_name}`",
                data=rows,
                column_names=[
                    'timestamp', 'directory_name', 'source_file', 'line_number', 'loaded_at',
                    'event_name', 'level', 'duration_ms', 'p_processName', 't_computerName',
                    't_connectID', 'usr', 'dbpid', 'osThread', 'sessionID', 'trans', 'func',
                    'tableName', 'context', 'sql', 'sdbl', 'planSQLText', 'exception',
                    'locks', 'waitConnections', 'deadlockConnectionIntersections',
                    'lkaid', 'lka', 'lkp', 'lkpid', 'lksrc', 'rows', 'rowsAffected',
                    'description', 'data', 'severity', 'category', 'mssql_error_code'
                ]
            )
            logger.debug(f"✅ Вставлено {len(rows)} событий")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка вставки в ClickHouse: {e}")
            return False

    def close(self):
        """Закрыть соединение"""
        if self.client:
            self.client.close()
            logger.info("🔌 ClickHouse отключён")


# ============================================================================
# 🔧 ПАРСЕР ФАЙЛОВ
# ============================================================================

def _extract_mssql_error_code(text: str) -> Optional[int]:
    """Извлечь код ошибки MSSQL из текста"""
    if not text:
        return None
    native_match = re.search(r'native=(\d+)', text, re.IGNORECASE)
    if native_match:
        return int(native_match.group(1))
    error_match = re.search(r'Error[:\s]+(\d+)', text, re.IGNORECASE)
    if error_match:
        return int(error_match.group(1))
    state_match = re.search(r'SQLState[=:]?(\d{5})', text, re.IGNORECASE)
    if state_match:
        return int(state_match.group(1))
    return None


def _detect_encoding(fp: Path) -> str:
    """Определить кодировку файла"""
    try:
        with open(fp, 'rb') as f:
            raw = f.read(10000)
            if raw.startswith(UTF8_BOM):
                raw = raw[3:]
            result = chardet.detect(raw)
            if result and result['confidence'] > 0.7:
                enc = result['encoding']
                if enc in POSSIBLE_ENCODINGS:
                    return enc
            for enc in POSSIBLE_ENCODINGS:
                try:
                    raw.decode(enc)
                    return enc
                except:
                    continue
            return 'utf-8-sig'
    except Exception as e:
        logger.warning(f"[{fp.name}] Ошибка определения кодировки: {e}")
        return 'utf-8-sig'


def _parse_date_from_filename(fn: str) -> Optional[datetime]:
    """Извлечь дату из имени файла (формат: YYMMDDHH.log)"""
    import re
    basename = os.path.basename(fn)
    match = re.match(r'(\d{2})(\d{2})(\d{2})(\d{2})\.log', basename)
    if match:
        yy, mm, dd, hh = match.groups()
        year = 2000 + int(yy)
        return datetime(year, int(mm), int(dd), int(hh))
    return None


def _parse_props(prop_str: str) -> Dict[str, str]:
    """Распарсить свойства события"""
    props = {}
    if not prop_str.strip():
        return props
    key = None
    val = []
    in_q = False
    qc = None
    i = 0
    while i < len(prop_str):
        c = prop_str[i]
        if c in '"\'':
            if not in_q:
                in_q = True
                qc = c
            elif c == qc:
                if i + 1 < len(prop_str) and prop_str[i + 1] == c:
                    val.append(c)
                    i += 1
                else:
                    in_q = False
                    qc = None
                    val.append(c)
        elif c == ',' and not in_q:
            if key:
                v = ''.join(val).strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in '"\'':
                    v = v[1:-1]
                props[key] = v
                key = None
                val = []
        elif c == '=' and key is None:
            key = ''.join(val).strip()
            val = []
        else:
            val.append(c)
        i += 1
    if key and val:
        v = ''.join(val).strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in '"\'':
            v = v[1:-1]
        props[key] = v
    return props


def _build_event(timestamp: datetime, event_name: str, level: int,
                 duration: int, props_str: str, line_num: int,
                 file_path: Path, directory_name: str) -> Optional[Dict[str, Any]]:
    """Построить событие из сырых данных"""
    try:
        props = _parse_props(props_str)
        sdbl_text = props.get('Sdbl', '')
        context_text = props.get('Context', '')
        sql_text = props.get('Sql', props.get('SQL', ''))

        mssql_error_code = None
        for text_field in [sdbl_text, context_text, sql_text]:
            if text_field:
                mssql_error_code = _extract_mssql_error_code(text_field)
                if mssql_error_code:
                    break

        return {
            'timestamp': timestamp,
            'directory_name': directory_name,
            'source_file': file_path.name,
            'line_number': line_num,
            'event_name': event_name,
            'level': level,
            'duration_ms': duration / 1000000,
            'p_processName': props.get('p:processName', ''),
            't_computerName': props.get('t:computerName', ''),
            't_connectID': props.get('t:connectID', ''),
            'usr': props.get('Usr', ''),
            'dbpid': props.get('dbpid', ''),
            'osThread': props.get('OSThread', ''),
            'sessionID': props.get('SessionID', ''),
            'trans': props.get('Trans', ''),
            'func': props.get('Func', ''),
            'tableName': props.get('tableName', ''),
            'context': props.get('Context', '')[:100000],
            'sql': props.get('Sql', props.get('SQL', ''))[:100000],
            'sdbl': props.get('Sdbl', '')[:100000],
            'planSQLText': props.get('planSQLText', '')[:100000],
            'exception': props.get('Exception', '')[:100000],
            'locks': props.get('Locks', ''),
            'waitConnections': props.get('WaitConnections', ''),
            'deadlockConnectionIntersections': props.get('DeadlockConnectionIntersections', ''),
            'lkaid': props.get('lkaid', ''),
            'lka': props.get('lka', ''),
            'lkp': props.get('lkp', ''),
            'lkpid': props.get('lkpid', ''),
            'lksrc': props.get('lksrc', ''),
            'rows': props.get('Rows', ''),
            'rowsAffected': props.get('RowsAffected', ''),
            'description': props.get('Description', ''),
            'data': props.get('Data', '')[:100000],
            'severity': 'info',
            'category': 'other',
            'mssql_error_code': mssql_error_code
        }
    except Exception as e:
        logger.debug(f"Ошибка построения события: {e}")
        return None


# parser.py — ИСПРАВЛЕННЫЙ parse_file()

def parse_file(file_path: Path, directory_name: str,
               ch_loader: ClickHouseLoader, batch_size: int,
               start_line: int = 0) -> Tuple[int, int, Optional[datetime], bool]:
    """
    Распарсить файл и вставить события в ClickHouse.
    """
    logger.info(f"📖 Парсинг: {file_path.name} (dir: {directory_name}, start_line: {start_line})")

    encoding = _detect_encoding(file_path)
    base_date = _parse_date_from_filename(str(file_path))
    if not base_date:
        logger.warning(f"⚠️ Не удалось определить дату из имени файла: {file_path.name}")
        return (0, 0, None, False)

    events_count = 0
    batch = []
    current_header = None
    current_lines = []
    match_num = 0
    last_line_number = start_line  # 🔧 Инициализируем start_line
    last_event_timestamp = None

    lines_skipped = 0
    lines_processed = 0
    current_event_start_line = 0

    try:
        with open(file_path, 'rb', buffering=1048576) as f:
            for line_number, line_bytes in enumerate(f, 1):
                # 🔧 Пропускаем уже обработанные строки
                if line_number <= start_line:
                    lines_skipped += 1
                    continue

                if line_number == 1 and line_bytes.startswith(UTF8_BOM):
                    line_bytes = line_bytes[len(UTF8_BOM):]

                lines_processed += 1
                last_line_number = line_number  # 🔧 Это номер строки В ФАЙЛЕ (1, 2, 3...)

                header_match = EVENT_HEADER_PATTERN.match(line_bytes)

                if header_match:
                    if current_header:
                        match_num += 1
                        try:
                            minute = int(current_header.group(1))
                            second = int(current_header.group(2))
                            microseconds = current_header.group(3).decode(encoding, errors='replace')
                            duration = int(current_header.group(4).decode(encoding, errors='replace'))
                            event_name = current_header.group(5).decode(encoding, errors='replace')
                            level = int(current_header.group(6).decode(encoding, errors='replace'))
                            properties_start = current_header.group(7).decode(encoding, errors='replace')

                            microsec = microseconds.ljust(6, '0')[:6]
                            event_timestamp = base_date.replace(
                                minute=minute, second=second, microsecond=int(microsec)
                            )

                            last_event_timestamp = event_timestamp

                            full_props = properties_start
                            if current_lines:
                                content_decoded = '\n'.join(
                                    line.decode(encoding, errors='replace') for line in current_lines
                                )
                                full_props = properties_start + '\n' + content_decoded

                            # 🔧 ИСПРАВЛЕНИЕ: используем current_event_start_line
                            event = _build_event(
                                event_timestamp, event_name, level, duration,
                                full_props, current_event_start_line, file_path, directory_name
                            )
                            if event:
                                batch.append(event)
                                events_count += 1
                                if len(batch) >= batch_size:
                                    if ch_loader.insert_batch(batch):
                                        logger.debug(f"✅ Батч {len(batch)} вставлен")
                                    batch = []
                                    gc.collect()
                        except Exception as e:
                            logger.debug(f"❌ Ошибка парсинга события #{match_num}: {e}")

                    current_header = header_match
                    current_lines = []
                    current_event_start_line = line_number  # ← ДОБАВИТЬ ЭТУ СТРОКУ
                elif current_header:
                    current_lines.append(line_bytes.rstrip(b'\n\r'))

        # 🔧 Обработка последнего события
        if current_header:
            match_num += 1
            try:
                minute = int(current_header.group(1))
                second = int(current_header.group(2))
                microseconds = current_header.group(3).decode(encoding, errors='replace')
                duration = int(current_header.group(4).decode(encoding, errors='replace'))
                event_name = current_header.group(5).decode(encoding, errors='replace')
                level = int(current_header.group(6).decode(encoding, errors='replace'))
                properties_start = current_header.group(7).decode(encoding, errors='replace')

                microsec = microseconds.ljust(6, '0')[:6]
                event_timestamp = base_date.replace(
                    minute=minute, second=second, microsecond=int(microsec)
                )

                last_event_timestamp = event_timestamp

                full_props = properties_start
                if current_lines:
                    content_decoded = '\n'.join(
                        line.decode(encoding, errors='replace') for line in current_lines
                    )
                    full_props = properties_start + '\n' + content_decoded

                # 🔧 ВАЖНО: line_number — это номер строки В ФАЙЛЕ
                event = _build_event(
                    event_timestamp, event_name, level, duration,
                    full_props, current_event_start_line, file_path, directory_name
                )
                if event:
                    batch.append(event)
                    events_count += 1
            except Exception as e:
                logger.debug(f"❌ Ошибка последнего события: {e}")

        # 🔧 Вставка остатка
        if batch:
            if ch_loader.insert_batch(batch):
                logger.debug(f"✅ Финальный батч {len(batch)} вставлен")
            batch = []
            gc.collect()

        #  Логирование статистики
        logger.info(
            f"✅ {file_path.name}: {events_count:,} событий (пропущено {lines_skipped} строк, обработано {lines_processed} строк, строки {start_line + 1}-{last_line_number})")

        # 🔧 Определяем is_complete
        if lines_processed == 0 and events_count == 0:
            is_complete = True
        else:
            is_complete = False

        return (events_count, last_line_number, last_event_timestamp, is_complete)

    except Exception as e:
        logger.error(f"❌ Ошибка парсинга {file_path}: {e}")
        return (0, start_line, None, False)



# ============================================================================
# 🔧 MAIN PARSER CLASS
# ============================================================================

class TJLogDaemon:
    """Основной класс фонового парсера"""

    def __init__(self):
        self.root_dir = Path(settings.parser_root_directory)
        self.watch_dirs = [
            "SQL_Locks", "1C_Culprits", "Trans", "1C_Victims",
            "47044381-d7a4-4e94-af24-fe6fc38b754a",
            "a76a488a-ceaf-4640-acf8-25fcf0fd68d3",
        ]
        self.excluded_dirs = set([
            d.strip() for d in settings.parser_excluded_dirs.split(',') if d.strip()
        ])
        self.poll_interval = settings.parser_poll_interval
        self.batch_size = settings.parser_batch_size

        self.state = StateManager(settings.parser_state_db_path)

        ch_config = {
            'host': settings.clickhouse_host,
            'port': settings.clickhouse_port,
            'username': settings.clickhouse_user,
            'password': settings.clickhouse_password,
            'database': settings.clickhouse_database
        }
        self.ch_loader = ClickHouseLoader(ch_config, settings.clickhouse_table_name)

        self._shutdown = False

        logger.info(f" TJLogDaemon инициализирован")
        logger.info(f"📁 Root: {self.root_dir}")
        logger.info(f" Watch dirs: {self.watch_dirs}")
        logger.info(f"⏱ Poll interval: {self.poll_interval}s")

    def _find_new_files(self) -> Generator[tuple, None, None]:
        """Найти новые необработанные файлы"""
        if not self.root_dir.exists():
            logger.error(f"❌ Root directory not found: {self.root_dir}")
            return

        for watch_dir in self.watch_dirs:
            dir_path = self.root_dir / watch_dir
            if not dir_path.exists():
                logger.warning(f"⚠️ Directory not found: {dir_path}")
                continue

            logger.debug(f"🔍 Scanning: {dir_path}")

            for log_file in dir_path.rglob("*.log"):
                file_parts = log_file.parts
                if any(excluded in file_parts for excluded in self.excluded_dirs):
                    continue

                if self.state.is_processed(log_file):
                    continue

                try:
                    rel_path = log_file.relative_to(dir_path)
                    if rel_path.parent == Path('.'):
                        directory_name = watch_dir
                    else:
                        directory_name = f"{watch_dir}/{rel_path.parent}"
                except ValueError:
                    directory_name = watch_dir

                yield log_file, directory_name

    def _scan_and_parse(self, stats_id: int):
        """Один цикл сканирования и парсинга"""
        logger.info("🔄 Starting scan cycle")
        start_time = time.time()

        files_found = 0
        files_processed = 0
        total_events = 0
        errors = 0

        for file_path, directory_name in self._find_new_files():
            files_found += 1
            try:
                start_line = self.state.get_last_line_number(file_path)

                events, last_line, last_ts, is_complete = parse_file(
                    file_path, directory_name, self.ch_loader, self.batch_size, start_line
                )

                self.state.mark_processed(
                    file_path, events,
                    last_line_number=last_line,
                    last_event_timestamp=last_ts,
                    is_complete=is_complete
                )

                if events >= 0 or last_line > 0:
                    files_processed += 1
                    total_events += events

            except Exception as e:
                logger.error(f"❌ Ошибка обработки {file_path}: {e}")
                errors += 1
                self.state.update_scan(stats_id, errors_count=errors,
                                       error_message=str(e)[:500])

            if files_found % 10 == 0:
                self.state.update_scan(stats_id,
                                       files_scanned=files_found,
                                       files_processed=files_processed,
                                       total_events=total_events)

        elapsed = time.time() - start_time
        self.state.update_scan(stats_id,
                               files_scanned=files_found,
                               files_processed=files_processed,
                               total_events=total_events,
                               errors_count=errors)

        logger.info(
            f"📊 Scan cycle completed: "
            f"{files_found} new files, "
            f"{files_processed} processed, "
            f"{total_events:,} events, "
            f"{errors} errors, "
            f"{elapsed:.1f}s"
        )

    def run(self):
        """Основной цикл демона"""
        logger.info("🚀 TJLogDaemon started")

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        stats = self.state.get_stats()
        logger.info(f"📈 Stats: {stats['total_processed']} files, {stats['total_events']:,} events")

        try:
            while not self._shutdown:
                stats_id = self.state.start_scan()
                self._scan_and_parse(stats_id)

                if self._shutdown:
                    break

                if datetime.now().hour == 3 and datetime.now().minute < 5:
                    self.state.cleanup_inactive(days=7)

                logger.info(f"😴 Sleeping {self.poll_interval}s...")
                for _ in range(self.poll_interval):
                    if self._shutdown:
                        break
                    time.sleep(1)

        except Exception as e:
            logger.exception(f"❌ Fatal error: {e}")
        finally:
            self._shutdown_handler()

    def _handle_shutdown(self, signum, frame):
        """Обработчик сигнала завершения"""
        sig_name = signal.Signals(signum).name
        logger.info(f"🛑 Received {sig_name}, shutting down...")
        self._shutdown = True

    def _shutdown_handler(self):
        """Завершение работы"""
        logger.info("🔌 Shutting down...")
        if self.ch_loader:
            self.ch_loader.close()
        if self.state:
            self.state.close()

        stats = self.state.get_stats() if self.state else {}
        logger.info(f"📈 Final stats: {stats.get('total_processed', 0)} files, {stats.get('total_events', 0):,} events")
        logger.info("✅ TJLogDaemon stopped")


# ============================================================================
# 🔧 ENTRY POINT
# ============================================================================

def main():
    """Точка входа"""
    init_database(settings.parser_state_db_path)

    daemon = TJLogDaemon()
    daemon.run()


if __name__ == "__main__":
    main()
