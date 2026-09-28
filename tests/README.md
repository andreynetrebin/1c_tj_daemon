# Тестовая подсистема 1C TJ Daemon

## Структура тестов

tests/
├── conftest.py # Общие фикстуры
├── fixtures/ # Тестовые данные
│ ├── 26092610_*.log # Синтетические фикстуры
│ ├── 26092616_real.log # Реальный лог ТЖ
│ └── 26092616_real_tail.log # Реальный лог с хвостом
├── test_parser_core.py # Unit-тесты parser.py
├── test_parser_legacy.py # Тесты багов parser.py
├── test_database.py # Unit-тесты StateManager
├── test_real_logs.py # Тесты на реальных логах
├── test_performance.py # Тесты производительности
└── test_integration.py # Интеграционные тесты


## Запуск тестов

### Все тесты
```bash
pytest tests/ -v


pytest tests/test_parser_core.py -v
pytest tests/test_database.py -v
pytest tests/test_real_logs.py -v

pytest tests/test_performance.py -v -s

pytest tests/ -v --tb=long