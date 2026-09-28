"""
Фикстуры для тестов подсистемы 1c_tj_daemon.
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

FIXTURES_DIR = Path(__file__).parent / "fixtures"

@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR

@pytest.fixture
def single_event_file() -> Path:
    """Файл с одним событием SDBL (имя в формате 1С: YYMMDDHH.log)"""
    return FIXTURES_DIR / "26092610.log"

@pytest.fixture
def multi_event_file() -> Path:
    """Файл с несколькими событиями разных типов"""
    return FIXTURES_DIR / "26092611.log"

@pytest.fixture
def tail_event_file() -> Path:
    """Файл начинается с 'хвоста' предыдущего события"""
    return FIXTURES_DIR / "26092612.log"

@pytest.fixture
def partial_event_file() -> Path:
    """Файл с одним незавершённым событием"""
    return FIXTURES_DIR / "26092613.log"

@pytest.fixture
def real_fragment_file() -> Path:
    """Реальный фрагмент ТЖ"""
    return FIXTURES_DIR / "26092614.log"

@pytest.fixture
def real_log_26092616() -> Path:
    return FIXTURES_DIR / "26092616.log"

@pytest.fixture
def real_log_26092616_tail() -> Path:
    return FIXTURES_DIR / "26092617.log"


@pytest.fixture
def empty_log_file(tmp_path) -> Path:
    f = tmp_path / "26092615.log"  # <-- Исправлено имя
    f.write_bytes(b"")
    return f

@pytest.fixture
def garbage_only_file(tmp_path) -> Path:
    f = tmp_path / "26092615.log"  # <-- Исправлено имя
    f.write_bytes(
        b": 5277 : some garbage line\n"
        b": 1 : another garbage line\n"
        b"Module : 136 : BeginTransaction() ;'\n"
    )
    return f

@pytest.fixture
def dynamic_log_file(tmp_path) -> Path:
    f = tmp_path / "26092615.log"  # <-- Исправлено имя
    f.write_bytes(
        b"00:01.572065-0,SDBL,4,p:processName=test_app,"
        b"t:computerName=TestSrv,t:connectID=100,Usr=TestUser,"
        b"Func=BeginTransaction\n"
    )
    return f


@pytest.fixture
def real_log_26092618() -> Path:
    """Экспериментальный реальный лог 26092618.log"""
    return FIXTURES_DIR / "26092618.log"

class MockClickHouseLoader:
    def __init__(self):
        self.inserted_batches = []
        self.total_inserted = 0
        self.insert_call_count = 0
        self._fail_on_insert = False

    def insert_batch(self, batch):
        if self._fail_on_insert:
            return False
        self.inserted_batches.append(list(batch))
        self.total_inserted += len(batch)
        self.insert_call_count += 1
        return True

    def close(self):
        pass

    def set_fail_mode(self, fail: bool):
        self._fail_on_insert = fail

    def get_all_events(self):
        result = []
        for batch in self.inserted_batches:
            result.extend(batch)
        return result

@pytest.fixture
def mock_ch_loader():
    return MockClickHouseLoader()