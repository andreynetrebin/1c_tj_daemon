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
import codecs
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

# parser.py — добавить в секцию импортов
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # для Python < 3.9

# Moscow timezone — тот же, что используется в ClickHouse
MOSCOW_TZ = ZoneInfo('Europe/Moscow')


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
                timestamp DateTime64(6, 'Europe/Moscow') CODEC(Delta(8), ZSTD(1)),
                directory_name LowCardinality(String),
                source_file String,
                line_number UInt64 CODEC(ZSTD(1)),
                loaded_at DateTime64(6, 'Europe/Moscow') CODEC(Delta(8), ZSTD(1)),
                event_name LowCardinality(String),
                level UInt8,
                duration_sec Float64,
                p_processName LowCardinality(String),
                t_computerName String,
                t_connectID Nullable(UInt32),
                usr LowCardinality(String),
                dbpid Nullable(UInt16),
                osThread Nullable(UInt32),
                sessionID String,

                sessionID_main Nullable(UInt64) ALIAS accurateCastOrNull(extract(sessionID, '^([0-9]+)(?:[(][0-9]+[)])?$'), 'UInt64'),
                sessionID_extra Nullable(UInt64) ALIAS accurateCastOrNull(extract(sessionID, '^[0-9]+[(]([0-9]+)[)]$'), 'UInt64'),
                trans Nullable(UInt8),
                func LowCardinality(String),
                module LowCardinality(String),
                method LowCardinality(String),
                memory Nullable(Int64),
                memoryPeak Nullable(UInt64),
                inBytes Nullable(UInt64),
                outBytes Nullable(UInt64),
                cpuTime_sec Nullable(Float64),
                tableName LowCardinality(String),
                context String CODEC(ZSTD(3)),
                sql String CODEC(ZSTD(3)),
                sdbl String CODEC(ZSTD(3)),
                planSQLText String CODEC(ZSTD(3)),
                exception String CODEC(ZSTD(3)),
                locks String CODEC(ZSTD(3)),
                regions String,
                waitConnections String,
                deadlockConnectionIntersections String,
                lkaid String,
                lka String,
                lkp String,
                lkpid String,
                lksrc String,
                rows Nullable(UInt64),
                rowsAffected Nullable(Int64),
                description String,
                data String CODEC(ZSTD(3)),
                severity LowCardinality(String),
                category LowCardinality(String),
                INDEX idx_en event_name TYPE bloom_filter GRANULARITY 8192,
                INDEX idx_usr usr TYPE bloom_filter GRANULARITY 8192
            )
            ENGINE = MergeTree()
            PARTITION BY toDate(timestamp)
            ORDER BY (timestamp, event_name)
            TTL timestamp + INTERVAL 30 DAY
            SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
        """)
        # Existing installations may already have the table without this property.
        self.client.command(f"ALTER TABLE {db}.{quoted} ADD COLUMN IF NOT EXISTS regions String AFTER locks")
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
                    datetime.now(MOSCOW_TZ),
                    e.get('event_name', ''),
                    int(e.get('level', 0) or 0),
                    float(e.get('duration_sec', 0) or 0),
                    e.get('p_processName', ''),
                    e.get('t_computerName', ''),
                    e.get('t_connectID'),
                    e.get('usr', ''),
                    e.get('dbpid'),
                    e.get('osThread'),
                    e.get('sessionID', ''),
                    e.get('trans'),
                    e.get('func', ''),
                    e.get('module', ''),
                    e.get('method', ''),
                    e.get('memory'),
                    e.get('memoryPeak'),
                    e.get('inBytes'),
                    e.get('outBytes'),
                    e.get('cpuTime_sec'),
                    e.get('tableName', ''),
                    (e.get('context') or '')[:100000],
                    (e.get('sql') or '')[:100000],
                    (e.get('sdbl') or '')[:100000],
                    (e.get('planSQLText') or '')[:100000],
                    (e.get('exception') or '')[:100000],
                    e.get('locks', ''),
                    e.get('regions', ''),
                    e.get('waitConnections', ''),
                    e.get('deadlockConnectionIntersections', ''),
                    e.get('lkaid', ''),
                    e.get('lka', ''),
                    e.get('lkp', ''),
                    e.get('lkpid', ''),
                    e.get('lksrc', ''),
                    e.get('rows'),
                    e.get('rowsAffected'),
                    e.get('description', ''),
                    (e.get('data') or '')[:100000],
                    e.get('severity', 'info'),
                    e.get('category', 'other'),
                ])

            self.client.insert(
                table=f"`{self.table_name}`",
                data=rows,
                column_names=[
                    'timestamp', 'directory_name', 'source_file', 'line_number', 'loaded_at',
                    'event_name', 'level', 'duration_sec', 'p_processName', 't_computerName',
                    't_connectID', 'usr', 'dbpid', 'osThread', 'sessionID', 'trans', 'func',
                    'module', 'method', 'memory', 'memoryPeak', 'inBytes', 'outBytes', 'cpuTime_sec',
                    'tableName', 'context', 'sql', 'sdbl', 'planSQLText', 'exception',
                    'locks', 'regions', 'waitConnections', 'deadlockConnectionIntersections',
                    'lkaid', 'lka', 'lkp', 'lkpid', 'lksrc', 'rows', 'rowsAffected',
                    'description', 'data', 'severity', 'category'
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

def _nullable_int(value, type_name):
    """Convert integer properties, preserving NULL and guarding overflow."""
    if value is None or not str(value).strip():
        return None
    raw = str(value).strip()
    signed = type_name.startswith('Int')
    bits = int(type_name.replace('UInt', '').replace('Int', ''))
    try:
        if not re.fullmatch(r'[+-]?[0-9]+', raw):
            raise ValueError('not an integer')
        number = int(raw)
        low = -(1 << (bits - 1)) if signed else 0
        high = (1 << (bits - int(signed))) - 1
        if low <= number <= high:
            return number
    except ValueError:
        pass
    logger.warning('Invalid %s TJ property: %r', type_name, raw[:100])
    return None


def _cpu_seconds(value):
    micros = _nullable_int(value, 'UInt64')
    return None if micros is None else micros / 1_000_000


def _detect_encoding(fp: Path) -> str:
    """Определить кодировку файла"""
    try:
        with open(fp, 'rb') as f:
            raw = f.read(10000)
            if raw.startswith(UTF8_BOM):
                return 'utf-8'
            try:
                codecs.getincrementaldecoder('utf-8')().decode(raw, final=False)
                return 'utf-8'
            except UnicodeDecodeError:
                pass
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


def _is_active_log(file_path: Path) -> bool:
    """
    Проверяет, является ли лог-файл "активным" (относится к текущему часу).
    """
    log_dt = _parse_date_from_filename(str(file_path))
    if not log_dt:
        return False

    # Приводим время из имени файла к часовому поясу Москвы
    log_dt_msk = log_dt.replace(tzinfo=MOSCOW_TZ)
    now_msk = datetime.now(MOSCOW_TZ)

    # Сравниваем год, месяц, день и час
    return (log_dt_msk.year == now_msk.year and
            log_dt_msk.month == now_msk.month and
            log_dt_msk.day == now_msk.day and
            log_dt_msk.hour == now_msk.hour)


def _is_active_log(file_path: Path) -> bool:
    """
    Проверить, является ли лог-файл активным (пишется 1С прямо сейчас).

    Активный лог — это файл, час которого (из имени YYMMDDHH.log)
    совпадает с текущим часом в часовом поясе Moscow.

    Returns:
        True  — файл активный, его НЕ НУЖНО обрабатывать сейчас
        False — файл завершён (или имя некорректное), можно обрабатывать
    """
    try:
        log_datetime = _parse_date_from_filename(str(file_path))
        if log_datetime is None:
            # Не смогли распарсить имя — считаем неактивным, пусть парсер сам разберётся
            return False

        # Приводим к Moscow timezone для корректного сравнения
        now_moscow = datetime.now(MOSCOW_TZ)
        log_moscow = log_datetime.replace(tzinfo=MOSCOW_TZ)

        # Сравниваем часы. Если совпадают — файл активный
        is_active = (log_moscow.year == now_moscow.year and
                     log_moscow.month == now_moscow.month and
                     log_moscow.day == now_moscow.day and
                     log_moscow.hour == now_moscow.hour)

        if is_active:
            logger.debug(f"⏭ Пропускаем активный лог: {file_path.name} (час {log_moscow.hour}:00)")

        return is_active
    except Exception as e:
        logger.warning(f"⚠️ Ошибка проверки активности {file_path}: {e}")
        return False


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



# Keep the existing text-field storage policy, but apply it during parsing.
_TRUNCATED_PROPERTIES = {'Context', 'Sql', 'SQL', 'Sdbl', 'planSQLText', 'Exception', 'Data'}
_STORED_PROPERTIES = _TRUNCATED_PROPERTIES | {
    'p:processName', 't:computerName', 't:connectID', 'Usr', 'dbpid',
    'OSThread', 'SessionID', 'Trans', 'Func', 'Module', 'Method', 'Memory',
    'MemoryPeak', 'InBytes', 'OutBytes', 'CpuTime', 'tableName', 'Locks', 'Regions',
    'WaitConnections', 'DeadlockConnectionIntersections', 'lkaid', 'lka',
    'lkp', 'lkpid', 'lksrc', 'Rows', 'RowsAffected', 'Description'
}
_KEY_TOKEN = re.compile(r"""['",=]""")
_VALUE_TOKEN = re.compile(r"""['",]""")


class StreamingProperties:
    """Bound retained text independently of the raw event size.

    feed() receives one physical line (or its header properties), with newline
    separators supplied by parse_file. The quote rules match the legacy parser.
    """
    def __init__(self, max_retained_chars=8 * 1024 * 1024):
        self.props = {}
        self.key = None
        self.quote = None
        self.pending_quote = False
        self.parts = []
        self.size = 0
        self.retained = 0
        self.max_retained = max_retained_chars
        self.truncated = set()

    def _append(self, text, start, end):
        if start == end:
            return
        if self.key is not None and self.key not in _STORED_PROPERTIES:
            return
        limit = (1024 if self.key is None else
                 100000 if self.key in _TRUNCATED_PROPERTIES else self.max_retained)
        remaining = min(limit - self.size, self.max_retained - self.retained - self.size)
        length = end - start
        if length > remaining:
            if self.key not in _TRUNCATED_PROPERTIES:
                raise ValueError('Retained property limit exceeded: %r' % self.key)
            self.truncated.add(self.key)
        take = min(length, max(0, remaining))
        if take:
            self.parts.append(text[start:start + take])
            self.size += take

    def _reset(self):
        self.parts = []
        self.size = 0

    def _store(self):
        if self.key in _STORED_PROPERTIES:
            value = ''.join(self.parts).strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            self.retained -= len(self.props.get(self.key, ''))
            self.props[self.key] = value
            self.retained += len(value)
        self.key = None
        self._reset()

    def feed(self, text):
        if not text:
            return
        i = 0
        if self.pending_quote:
            self.pending_quote = False
            if text[0] == self.quote:
                self._append(text, 0, 1)
                i = 1
            else:
                self._append(self.quote, 0, 1)
                self.quote = None
        end = len(text)
        while i < end:
            if self.quote is not None:
                j = text.find(self.quote, i)
                if j < 0:
                    self._append(text, i, end)
                    return
                self._append(text, i, j)
                if j + 1 == end:
                    self.pending_quote = True
                    return
                if text[j + 1] == self.quote:
                    self._append(text, j, j + 1)
                    i = j + 2
                else:
                    self._append(text, j, j + 1)
                    self.quote = None
                    i = j + 1
                continue
            token = (_KEY_TOKEN if self.key is None else _VALUE_TOKEN).search(text, i)
            if token is None:
                self._append(text, i, end)
                return
            j = token.start()
            self._append(text, i, j)
            c = text[j]
            if c in "\"'":
                self.quote = c
            elif c == '=' and self.key is None:
                self.key = ''.join(self.parts).strip()
                self._reset()
            elif c == ',' and self.key:
                self._store()
            i = j + 1

    def finish(self):
        if self.pending_quote:
            self._append(self.quote, 0, 1)
            self.pending_quote = False
            self.quote = None
        if self.key and self.size:
            self._store()
        return self.props


def _build_event(timestamp: datetime, event_name: str, level: int,
                 duration: int, props_str: str, line_num: int,
                 file_path: Path, directory_name: str) -> Optional[Dict[str, Any]]:
    """Построить событие из сырых данных"""
    try:
        props = props_str if isinstance(props_str, dict) else _parse_props(props_str)
        return {
            'timestamp': timestamp,
            'directory_name': directory_name,
            'source_file': file_path.name,
            'line_number': line_num,
            'event_name': event_name,
            'level': level,
            'duration_sec': duration / 1000000,
            'p_processName': props.get('p:processName', ''),
            't_computerName': props.get('t:computerName', ''),
            't_connectID': _nullable_int(props.get('t:connectID'), 'UInt32'),
            'usr': props.get('Usr', ''),
            'dbpid': _nullable_int(props.get('dbpid'), 'UInt16'),
            'osThread': _nullable_int(props.get('OSThread'), 'UInt32'),
            'sessionID': props.get('SessionID', ''),
            'trans': _nullable_int(props.get('Trans'), 'UInt8'),
            'func': props.get('Func', ''),
            'module': props.get('Module', ''),
            'method': props.get('Method', ''),
            'memory': _nullable_int(props.get('Memory'), 'Int64'),
            'memoryPeak': _nullable_int(props.get('MemoryPeak'), 'UInt64'),
            'inBytes': _nullable_int(props.get('InBytes'), 'UInt64'),
            'outBytes': _nullable_int(props.get('OutBytes'), 'UInt64'),
            'cpuTime_sec': _cpu_seconds(props.get('CpuTime')),
            'tableName': props.get('tableName', ''),
            'context': props.get('Context', '')[:100000],
            'sql': props.get('Sql', props.get('SQL', ''))[:100000],
            'sdbl': props.get('Sdbl', '')[:100000],
            'planSQLText': props.get('planSQLText', '')[:100000],
            'exception': props.get('Exception', '')[:100000],
            'locks': props.get('Locks', ''),
            'regions': props.get('Regions', ''),
            'waitConnections': props.get('WaitConnections', ''),
            'deadlockConnectionIntersections': props.get('DeadlockConnectionIntersections', ''),
            'lkaid': props.get('lkaid', ''),
            'lka': props.get('lka', ''),
            'lkp': props.get('lkp', ''),
            'lkpid': props.get('lkpid', ''),
            'lksrc': props.get('lksrc', ''),
            'rows': _nullable_int(props.get('Rows'), 'UInt64'),
            'rowsAffected': _nullable_int(props.get('RowsAffected'), 'Int64'),
            'description': props.get('Description', ''),
            'data': props.get('Data', '')[:100000],
            'severity': 'info',
            'category': 'other',
        }
    except Exception as e:
        logger.debug(f"Ошибка построения события: {e}")
        return None


# parser.py — ИСПРАВЛЕННЫЙ parse_file()

def parse_file(file_path: Path, directory_name: str,
               ch_loader: ClickHouseLoader, batch_size: int,
               start_line: int = 0, *, checkpoint=None,
               batch_max_bytes=None, max_line_bytes=None, should_stop=None,
               read_chunk_bytes=None, single_event_max_bytes=None, on_error=None
               ) -> Tuple[int, int, Optional[datetime], bool]:
    """Stream complete events; checkpoint only after acknowledged inserts."""
    logger.info("Parsing %s (dir: %s, start_line: %d)", file_path.name, directory_name, start_line)
    batch_max_bytes = (getattr(settings, 'parser_batch_max_bytes', 16 * 1024 * 1024)
                       if batch_max_bytes is None else batch_max_bytes)
    read_chunk_bytes = (getattr(settings, 'parser_read_chunk_bytes', 65536)
                        if read_chunk_bytes is None else read_chunk_bytes)
    single_event_max_bytes = (getattr(settings, 'parser_single_event_max_bytes', 128 * 1024 * 1024)
                              if single_event_max_bytes is None else single_event_max_bytes)
    if min(batch_size, batch_max_bytes, read_chunk_bytes, single_event_max_bytes) <= 0 or (max_line_bytes is not None and max_line_bytes <= 0):
        raise ValueError('Parser limits must be positive')
    encoding = _detect_encoding(file_path)
    base_date = _parse_date_from_filename(str(file_path))
    if not base_date:
        return (0, 0, None, False)
    batch = []
    batch_bytes = 0
    batch_end = start_line
    committed_line = start_line
    committed_events = 0
    committed_ts = None
    last_line = start_line
    stream = None
    header = None
    event_start = 0
    event_bytes = 0

    def flush():
        nonlocal batch, batch_bytes, committed_line, committed_events, committed_ts
        if not batch:
            return
        if not ch_loader.insert_batch(batch):
            raise RuntimeError('ClickHouse insert failed')
        ts = batch[-1]['timestamp']
        # The callback must persist the delta, not the cumulative event count.
        if checkpoint is not None:
            checkpoint(len(batch), batch_end, ts)
        committed_line = batch_end
        committed_events += len(batch)
        committed_ts = ts
        logger.debug("Inserted %d events, estimated %d bytes, checkpoint %d",
                     len(batch), batch_bytes, committed_line)
        batch = []
        batch_bytes = 0

    def finish_event(end_line):
        nonlocal batch_bytes, batch_end
        if stream is None:
            return
        minute, second, micro, duration, name, level = header
        ts = base_date.replace(minute=int(minute), second=int(second),
                               microsecond=int(micro.ljust(6, b'0')[:6]),
                               tzinfo=MOSCOW_TZ)
        event = _build_event(ts, name.decode(encoding, errors='replace'),
                             int(level), int(duration), stream.finish(),
                             event_start, file_path, directory_name)
        if event is None:
            raise ValueError('Cannot build event at line %d' % event_start)
        if stream.truncated:
            logger.warning("Truncated text fields in %s:%d (%d raw bytes, %d lines): %s",
                           file_path.name, event_start, event_bytes,
                           end_line - event_start + 1, ','.join(sorted(stream.truncated)))
        # Conservative estimate includes Python string storage and encoded data.
        size = 1024 + sum(8 * len(v) + 64 if isinstance(v, str) else 64
                          for v in event.values())
        if batch and (len(batch) >= batch_size or batch_bytes + size > batch_max_bytes):
            flush()
        if size > single_event_max_bytes:
            raise ValueError('Event exceeds single-event budget at line %d; largest fields: %s' % (event_start, sorted(((k, len(v)) for k, v in event.items() if isinstance(v, str)), key=lambda x: -x[1])[:5]))
        if size > batch_max_bytes:
            logger.info('Large single event %s:%d, estimated %d bytes', file_path.name, event_start, size)
        batch.append(event)
        batch_bytes += size
        batch_end = end_line
        if len(batch) >= batch_size or batch_bytes >= batch_max_bytes:
            flush()

    try:
        with open(file_path, 'rb', buffering=1048576) as f:
            line_number = 0
            at_start = True
            decoder = None
            pending_cr = b''
            line_size = 0
            while True:
                if should_stop is not None and should_stop():
                    flush()
                    return (committed_events, committed_line, committed_ts, False)
                raw = f.readline(read_chunk_bytes)
                if not raw:
                    if decoder is not None and stream is not None and line_number > start_line:
                        stream.feed(decoder.decode(pending_cr, final=True))
                    break
                first = at_start
                if first:
                    line_number += 1
                    line_size = 0
                line_size += len(raw)
                at_start = raw.endswith(b'\n')
                if max_line_bytes is not None and line_size > max_line_bytes:
                    raise ValueError('Physical line exceeds configured limit at line %d' % line_number)
                if line_number <= start_line:
                    continue
                last_line = line_number
                raw_size = len(raw)
                if first:
                    decoder = codecs.getincrementaldecoder(encoding)(errors='replace')
                    pending_cr = b''
                    if line_number == 1 and raw.startswith(UTF8_BOM):
                        raw = raw[len(UTF8_BOM):]
                    match = EVENT_HEADER_PATTERN.match(raw)
                    if match:
                        finish_event(line_number - 1)
                        header = match.groups()[:6]
                        stream = StreamingProperties(max_retained_chars=max(1, single_event_max_bytes // 8))
                        event_start = line_number
                        event_bytes = 0
                        # Use original bytes after header, not group(7), to keep
                        # newline handling identical for split and unsplit lines.
                        raw = raw[match.start(7):]
                    elif stream is not None:
                        stream.feed('\n')
                if stream is not None:
                    event_bytes += raw_size
                    data = pending_cr + raw
                    pending_cr = b''
                    if at_start:
                        data = data.rstrip(b'\r\n')
                    elif data.endswith(b'\r'):
                        data = data[:-1]
                        pending_cr = b'\r'
                    stream.feed(decoder.decode(data, final=at_start))
                if at_start:
                    decoder = None
        finish_event(last_line)
        flush()
        logger.info("%s: %d events, lines %d-%d", file_path.name, committed_events,
                    start_line + 1, last_line)
        return (committed_events, last_line, committed_ts, last_line == start_line)
    except Exception as exc:
        if on_error is not None:
            on_error(exc)
        logger.exception("Parsing stopped for %s at line %d; confirmed line %d",
                         file_path, last_line, committed_line)
        return (committed_events, committed_line, committed_ts, False)


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

                # 🔧 НОВОЕ: Пропускаем активные логи, если включена настройка
                if getattr(settings, 'parser_ignore_active_logs', True) and _is_active_log(log_file):
                    logger.debug(f"⏭ Пропуск активного лога: {log_file.name}")
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
            if self._shutdown:
                break
            files_found += 1
            try:
                start_line = self.state.get_last_line_number(file_path)

                def save_checkpoint(count, line, timestamp):
                    self.state.mark_processed(file_path, count, last_line_number=line,
                                              last_event_timestamp=timestamp, is_complete=False)

                parse_errors = []
                events, last_line, last_ts, is_complete = parse_file(
                    file_path, directory_name, self.ch_loader, self.batch_size, start_line,
                    checkpoint=save_checkpoint, should_stop=lambda: self._shutdown,
                    on_error=parse_errors.append
                )

                # Batch event counts were already persisted. Only finalize position.
                if last_line > start_line or is_complete:
                    self.state.mark_processed(
                        file_path, 0, last_line_number=last_line,
                        last_event_timestamp=last_ts, is_complete=is_complete
                    )

                total_events += events
                if parse_errors:
                    errors += 1
                    self.state.update_scan(stats_id, errors_count=errors,
                                           error_message=str(parse_errors[0])[:500])
                elif not self._shutdown:
                    files_processed += 1

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
            raise
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
