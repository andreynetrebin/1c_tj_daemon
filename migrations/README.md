# Переход на схему CALL (v2)

## Подготовка

- Для новой установки выполнить 001_create_v2.sql и 002_create_view.sql. Для действующей v2 с cpuTime в микросекундах использовать 003_cpu_time_seconds.sql до запуска нового парсера.
- Проверить SHOW CREATE TABLE и DESCRIBE TABLE для v2 и all. IF NOT EXISTS не исправляет уже существующую схему.
- Запустить python -m py_compile parser.py и python -m pytest -v.
- Убедиться, что каталоги с CALL входят в список watch_dirs в TJLogDaemon, а регистрация CALL включена на сервере 1С.
- Проверить, что аналитические запросы используют duration_sec и новые типы; для истории читать tj_events_ut_all.

## Переключение

1. Корректно остановить демон, дождаться завершения процесса. Регистрацию ТЖ оставить включённой, обеспечить хранение файлов до дочитывания.
2. Сохранить .env и SQLite state.db (после остановки, включая WAL/SHM, если существуют); не удалять и не сбрасывать состояние.
3. Развернуть проверенную версию кода. В действующей конфигурации .env явно задать CLICKHOUSE_TABLE_NAME=tj_events_ut_v2 и CLICKHOUSE_DATABASE=NetrebinAA. Проверить переменные окружения, которые могут переопределять .env.
4. Запустить только один экземпляр демона. Проверить журнал запуска: целевая таблица именно tj_events_ut_v2, нет ошибок вставки.
5. Проверить новые события и CALL:

```sql
SELECT timestamp, loaded_at, sessionID, module, method, duration_sec,
       memory, memoryPeak, inBytes, outBytes, cpuTime_sec
FROM NetrebinAA.tj_events_ut_v2
WHERE event_name = 'CALL'
ORDER BY timestamp DESC LIMIT 20;
```

```sql
SELECT event_name, count(), max(timestamp), max(loaded_at)
FROM NetrebinAA.tj_events_ut_v2
WHERE loaded_at >= now() - INTERVAL 15 MINUTE
GROUP BY event_name;
```

При отсутствии CALL проверить logcfg.xml, наблюдаемые каталоги и фильтр активных файлов (текущий час может не читаться до следующего часа).

## Откат

Остановить демон, вернуть предыдущую версию кода и прежнюю целевую таблицу. Сохранить актуальное состояние SQLite, чтобы не читать заново уже подтверждённые события. Новые данные оставить в v2 и продолжать читать объединяющее представление. Не восстанавливать старый checkpoint автоматически: это может создать дубликаты. При подозрении на частично подтверждённую вставку сверить исходные файлы и данные обеих таблиц.

MergeTree не гарантирует exactly-once: сбой после успешной вставки на сервере, но до получения ответа или записи checkpoint, может привести к повтору. Сохранение подтверждённой позиции снижает риск пропусков, но не устраняет риск дубликатов.

Старую таблицу не удалять до истечения TTL и проверки истории; перед удалением заменить объединяющее VIEW представлением только над v2. Не выполнять массовую миграцию старых данных.


## Потоковый парсер: защита от OOM

- Сырые многострочные события больше не объединяются в одну строку.
- Context, Sql/SQL, Sdbl, planSQLText, Exception, Data ограничены 100000 символами во время чтения. Состояние кавычек сохраняется, поэтому свойства после длинного SQL продолжают разбираться.
- Сокращение фиксируется WARNING с именем файла, начальной строкой, размером исходного события и списком полей. Исходный журнал не изменяется; сохраняйте его до окончания проверки.
- Пакет ограничен количеством событий и консервативной оценкой объёма памяти. Физические строки читаются фрагментами с инкрементальным декодированием и сохранением состояния кавычек. Крупные события отправляются отдельным INSERT. При превышении защитного бюджета одиночного события или несокращаемого свойства (включая Locks) обработка файла останавливается без продвижения за неподтверждённое событие; ошибка учитывается в статистике.
- SQLite обновляется после каждого подтверждённого пакета. Сбой между вставкой и записью SQLite по-прежнему может вызвать повтор.
- Настройки по умолчанию работают даже со старым Linux config.py; для настройки через .env добавьте в серверный класс Settings поля parser_batch_max_bytes, parser_read_chunk_bytes и parser_single_event_max_bytes из config.py, не заменяя Linux-пути Windows-путями.

Проверенные настройки PROD (25.11.2026):

```ini
PARSER_BATCH_SIZE=1000
PARSER_BATCH_MAX_BYTES=16777216
PARSER_READ_CHUNK_BYTES=65536
PARSER_SINGLE_EVENT_MAX_BYTES=536870912
PARSER_IGNORE_ACTIVE_LOGS=true
```

Проверка: python -m pytest tests/test_streaming_parser.py -v, затем весь набор тестов. На PROD сначала проверить отсутствие уже загруженных строк проблемного файла и сохранённый checkpoint, затем развернуть parser.py при остановленной службе. Не сбрасывать SQLite. Следить за VmRSS/VmHWM процесса, memory.events, memory.stat cgroup и журналом systemd. MemoryCurrent/MemoryPeak включают файловый кеш; бюджет одиночного события является оценкой, а не гарантией RSS. Новые колонки метрик размера события не обязательны и в данном изменении не добавлены.


## CPU time in seconds

Fresh installations use updated 001/002. Existing v2 installations with legacy cpuTime Nullable(UInt64) must stop the daemon, verify the old column contains microseconds only, then run 003_cpu_time_seconds.sql before deploying the new parser. Existing raw cpuTime is retained; cpuTime_sec is Nullable(Float64), computed from the old column for historical parts and explicitly inserted for new events. Do not rerun a materialization after deploying new code without reviewing ClickHouse version semantics. Update analytics to cpuTime_sec. Do not use old parser against a fresh seconds-only schema. Do not remove the legacy column while the DEFAULT references it.


## Результаты приёмки 25.11.2026

По журналам и SQL-проверкам оператора: 129 тестов прошли (7,38 с, одно предупреждение SQLAlchemy); файл 2,4 GiB — 41 событие, 41 уникальная начальная строка; после увеличения бюджета до 512 MiB цикл из 15 файлов завершён с 83 102 событиями и 0 ошибок за 413,8 с. VmHWM процесса 389160 KiB, VmRSS 98940 KiB, VmSwap 0. Среди CALL, загруженных после 09:24:05 MSK, 4742 из 4742 имеют cpuTime_sec, старый cpuTime NULL у всех. В проверенной выборке новых вставок дубликатов нет (не является гарантией exactly-once или отсутствия пересечений с историей). Сохранять лимит systemd 4 GiB. Не удалять исходные журналы до подтверждения загрузки. Прежний CPU-столбец не удалять, пока на него ссылается DEFAULT.


## Regions property

Existing installations must stop the daemon and apply `004_add_regions.sql` to the configured ClickHouse database/table before or alongside deployment. The parser also issues an idempotent `ADD COLUMN IF NOT EXISTS` when ensuring the target table, so an existing table is upgraded automatically. Fresh installations include `regions String` in `001_create_v2.sql`. The property is stored as received from the TJ event header (for example `AccumRg44591.DIMS`).
