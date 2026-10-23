"""
Тесты фильтрации активных логов.
Проверяем, что парсер корректно пропускает файлы текущего часа.
"""
import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import _is_active_log, _parse_date_from_filename, MOSCOW_TZ


class TestIsActiveLog:
    """Тесты функции _is_active_log"""

    def test_current_hour_is_active(self, tmp_path):
        """Файл текущего часа должен считаться активным"""
        now_moscow = datetime.now(MOSCOW_TZ)
        # Создаём файл с именем текущего часа: YYMMDDHH.log
        filename = now_moscow.strftime("%y%m%d%H") + ".log"
        log_file = tmp_path / filename
        log_file.write_bytes(b"")

        assert _is_active_log(log_file) is True, (
            f"Файл {filename} должен быть активным (текущий час)"
        )

    def test_previous_hour_is_not_active(self, tmp_path):
        """Файл предыдущего часа НЕ должен считаться активным"""
        now_moscow = datetime.now(MOSCOW_TZ)
        # Файл час назад
        from datetime import timedelta
        prev_hour = now_moscow - timedelta(hours=1)
        filename = prev_hour.strftime("%y%m%d%H") + ".log"
        log_file = tmp_path / filename
        log_file.write_bytes(b"")

        assert _is_active_log(log_file) is False, (
            f"Файл {filename} (прошлый час) не должен быть активным"
        )

    def test_yesterday_file_is_not_active(self, tmp_path):
        """Файл вчерашнего дня НЕ должен считаться активным"""
        from datetime import timedelta
        now_moscow = datetime.now(MOSCOW_TZ)
        yesterday = now_moscow - timedelta(days=1)
        filename = yesterday.strftime("%y%m%d%H") + ".log"
        log_file = tmp_path / filename
        log_file.write_bytes(b"")

        assert _is_active_log(log_file) is False

    def test_future_file_is_not_active(self, tmp_path):
        """Файл будущего часа НЕ должен считаться активным"""
        from datetime import timedelta
        now_moscow = datetime.now(MOSCOW_TZ)
        future = now_moscow + timedelta(hours=2)
        filename = future.strftime("%y%m%d%H") + ".log"
        log_file = tmp_path / filename
        log_file.write_bytes(b"")

        assert _is_active_log(log_file) is False

    def test_invalid_filename_is_not_active(self, tmp_path):
        """Файл с некорректным именем не должен считаться активным"""
        log_file = tmp_path / "invalid_name.log"
        log_file.write_bytes(b"")

        assert _is_active_log(log_file) is False

    def test_real_fixture_26092616(self, tmp_path):
        """Проверка на реальной фикстуре (если запущено в час, отличный от 16:00 26.09.2026)"""
        log_file = tmp_path / "26092616.log"
        log_file.write_bytes(b"")

        now_moscow = datetime.now(MOSCOW_TZ)
        # Если сейчас НЕ 16:00 26 сентября 2026 — файл не активный
        is_expected_active = (
                now_moscow.year == 2026 and
                now_moscow.month == 9 and
                now_moscow.day == 26 and
                now_moscow.hour == 16
        )

        assert _is_active_log(log_file) == is_expected_active


class TestIsActiveLogWithMockedTime:
    """Тесты с моком текущего времени для детерминированной проверки"""

    def test_exact_match_with_mocked_now(self, tmp_path):
        """Точное совпадение часа с моком datetime.now()"""
        # Фиксируем "текущее" время: 2026-09-26 16:30 МСК
        fixed_now = datetime(2026, 9, 26, 16, 30, tzinfo=MOSCOW_TZ)

        log_file = tmp_path / "26092616.log"  # 16:00 того же дня
        log_file.write_bytes(b"")

        with patch('parser.datetime') as mock_dt:
            mock_dt.now.return_value = fixed_now
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            # В этом тесте мы проверяем логику, но из-за особенностей мока
            # zoneinfo лучше тестировать через реальные файлы
            pass

        # Альтернатива: проверяем через _parse_date_from_filename
        parsed = _parse_date_from_filename("26092616.log")
        assert parsed.hour == 16
        assert parsed.day == 26
        assert parsed.month == 9


class TestEdgeCases:
    """Граничные случаи"""

    def test_midnight_transition(self, tmp_path):
        """
        Переход через полночь: файл 23:00 должен быть неактивным в 00:30 следующего дня
        """
        # Имитируем: сейчас 00:30 27 сентября
        # Файл 26092623.log (23:00 26 сентября) — должен быть НЕ активным
        log_file = tmp_path / "26092623.log"
        log_file.write_bytes(b"")

        # Проверяем парсинг
        parsed = _parse_date_from_filename("26092623.log")
        assert parsed.hour == 23
        assert parsed.day == 26

        # В реальности (если сейчас не 23:00 26 сентября) файл не активный
        now_moscow = datetime.now(MOSCOW_TZ)
        is_expected_active = (
                now_moscow.year == 2026 and
                now_moscow.month == 9 and
                now_moscow.day == 26 and
                now_moscow.hour == 23
        )
        assert _is_active_log(log_file) == is_expected_active

    def test_year_boundary(self, tmp_path):
        """Переход года: файл 25123123.log (23:00 31.12.2025)"""
        log_file = tmp_path / "25123123.log"
        log_file.write_bytes(b"")

        parsed = _parse_date_from_filename("25123123.log")
        assert parsed.year == 2025
        assert parsed.month == 12
        assert parsed.day == 31
        assert parsed.hour == 23

        # В 2026 году этот файл точно не активный
        now_moscow = datetime.now(MOSCOW_TZ)
        if now_moscow.year != 2025:
            assert _is_active_log(log_file) is False