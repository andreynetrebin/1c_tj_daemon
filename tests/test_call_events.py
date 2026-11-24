from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from parser import _build_event, _nullable_int, parse_file, ClickHouseLoader

CALLS = [
    ("00:02.258031-1732972,CALL,1,process=rphost,OSThread=37576,t:clientID=402,callWait=0,first=true,Usr=DefUser,SessionID=48592,p:processName=Happywear_New,Func=Background job,Module=HW_РассылкаTelegramСервер,Method=HW_РассылкиTelegramФормированиеСообщений,Memory=163219,MemoryPeak=5544697,InBytes=0,OutBytes=0,CpuTime=109375", ("HW_РассылкаTelegramСервер", "HW_РассылкиTelegramФормированиеСообщений", 163219, 5544697, 0, 0, 109375, 1.732972)),
    ("00:02.789014-2278970,CALL,1,process=rphost,OSThread=42472,t:clientID=395,callWait=0,first=true,Usr=Администратор,SessionID=48580,p:processName=Happywear_New,Func=Background job,Module=ДополнительныеОтчетыИОбработки,Method=ВыполнитьОбработкуПоРегламентномуЗаданию,Memory=623523,MemoryPeak=11780887,InBytes=235238,OutBytes=218641,CpuTime=375000", ("ДополнительныеОтчетыИОбработки", "ВыполнитьОбработкуПоРегламентномуЗаданию", 623523, 11780887, 235238, 218641, 375000, 2.278970)),
]
FIELDS = ("module", "method", "memory", "memoryPeak", "inBytes", "outBytes", "cpuTime", "duration_sec")


@pytest.mark.parametrize("line,expected", CALLS)
def test_call_from_log(tmp_path, line, expected):
    path = tmp_path / "26092610.log"
    path.write_text(line + "\n", encoding="utf-8")
    loader = Mock()
    loader.insert_batch.return_value = True
    count, checkpoint, _, _ = parse_file(path, "test", loader, 100)
    assert (count, checkpoint) == (1, 1)
    event = loader.insert_batch.call_args.args[0][0]
    assert tuple(event[k] for k in FIELDS) == expected
    assert event["event_name"] == "CALL"
    assert "mssql_error_code" not in event
    assert "duration_ms" not in event
    assert isinstance(event["osThread"], int)


def test_missing_and_signed_properties():
    event = _build_event(datetime(2026, 9, 26), "CALL", 1, 0,
                         "SessionID=106088(106048),RowsAffected=-1,Memory=-12,InBytes=0",
                         1, Path("26092610.log"), "test")
    assert event["sessionID"] == "106088(106048)"
    assert event["rowsAffected"] == -1
    assert event["memory"] == -12
    assert event["inBytes"] == 0
    assert event["outBytes"] is None
    assert event["cpuTime"] is None
    assert event["module"] == ""
    assert event["t_connectID"] is None


@pytest.mark.parametrize("value,kind,expected", [
    ("", "UInt32", None), (None, "UInt32", None),
    ("0", "UInt8", 0), ("255", "UInt8", 255), ("256", "UInt8", None),
    ("65535", "UInt16", 65535), ("65536", "UInt16", None),
    ("4294967295", "UInt32", 4294967295), ("4294967296", "UInt32", None),
    ("18446744073709551615", "UInt64", 18446744073709551615),
    ("18446744073709551616", "UInt64", None),
    ("-9223372036854775808", "Int64", -9223372036854775808),
    ("9223372036854775808", "Int64", None),
    ("-1", "UInt32", None), ("abc", "UInt32", None),
])
def test_numeric(value, kind, expected):
    assert _nullable_int(value, kind) == expected


def test_retry_after_partial_success(tmp_path):
    path = tmp_path / "26092610.log"
    path.write_text("\n".join([CALLS[0][0], CALLS[1][0], CALLS[0][0]]) + "\n", encoding="utf-8")
    loader = Mock()
    loader.insert_batch.side_effect = [True, False]
    count, checkpoint, _, complete = parse_file(path, "test", loader, 1)
    assert (count, checkpoint, complete) == (1, 1, False)
    retry = Mock()
    retry.insert_batch.return_value = True
    count, checkpoint, _, _ = parse_file(path, "test", retry, 100, checkpoint)
    assert (count, checkpoint) == (2, 3)
    assert [e["line_number"] for e in retry.insert_batch.call_args.args[0]] == [2, 3]


def test_insert_columns():
    event = _build_event(datetime(2026, 9, 26), "CALL", 1, 1732972,
                         CALLS[0][0].split(",", 3)[3], 1, Path("26092610.log"), "test")
    loader = ClickHouseLoader.__new__(ClickHouseLoader)
    loader.table_name = "test"
    loader.client = Mock()
    assert loader.insert_batch([event])
    kwargs = loader.client.insert.call_args.kwargs
    columns, row = kwargs["column_names"], kwargs["data"][0]
    assert len(columns) == len(row)
    mapping = dict(zip(columns, row))
    assert tuple(mapping[k] for k in FIELDS) == CALLS[0][1]
    assert "mssql_error_code" not in mapping
