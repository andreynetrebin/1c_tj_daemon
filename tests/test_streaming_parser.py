from unittest.mock import Mock
import tracemalloc

from parser import parse_file, StreamingProperties, _parse_props


def test_streaming_legacy_properties():
    raw = "Usr=admin,Context='first,second',Rows=1,RowsAffected=-1,Locks='a''b'"
    stream = StreamingProperties()
    stream.feed(raw)
    expected = _parse_props(raw)
    assert stream.finish() == expected


def test_huge_sdbl_keeps_trailing_properties(tmp_path):
    path = tmp_path / "26112417.log"
    # More than 32 MiB raw data, generated without a giant in-memory string.
    with path.open("wb") as f:
        f.write(b"17:52.299004-213384997,SDBL,3,Usr=DefUser,Sdbl='INSERT #temp\n")
        for _ in range(40000):
            f.write(b"(" + b"x" * 1024 + b"),\n")
        f.write(b"',Rows=1,RowsAffected=4552832,Context='tail'\n")
        f.write(b"18:00.000001-1,CALL,1,Module=M,Method=F,Memory=12\n")
    loader = Mock()
    loader.insert_batch.return_value = True
    checkpoints = []
    tracemalloc.start()
    try:
        result = parse_file(path, "test", loader, 100,
                            checkpoint=lambda *args: checkpoints.append(args))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result[:2] == (2, 40003)
    rows = loader.insert_batch.call_args.args[0]
    assert len(rows[0]["sdbl"]) == 100000
    assert rows[0]["sdbl"].startswith("INSERT #temp")
    assert rows[0]["rows"] == 1
    assert rows[0]["rowsAffected"] == 4552832
    assert rows[0]["context"].startswith("tail")
    assert rows[1]["module"] == "M"
    assert rows[1]["memory"] == 12
    assert checkpoints[0][:2] == (2, 40003)
    assert peak < 16 * 1024 * 1024


def test_byte_budget_and_failed_checkpoint(tmp_path):
    path = tmp_path / "26112417.log"
    line = b"17:52.299004-1,CALL,1,Module=M,Method=F,Context='" + b"x" * 20000 + b"'\n"
    with path.open("wb") as f:
        for _ in range(4):
            f.write(line)
    loader = Mock()
    loader.insert_batch.side_effect = [True, False]
    checkpoints = []
    result = parse_file(path, "test", loader, 1000, batch_max_bytes=200000,
                        checkpoint=lambda *args: checkpoints.append(args))
    assert result[:2] == (1, 1)
    assert len(checkpoints) == 1
    assert checkpoints[0][:2] == (1, 1)
    assert all(len(c.args[0]) == 1 for c in loader.insert_batch.call_args_list)


def test_large_physical_line_fails_without_advancing(tmp_path):
    path = tmp_path / "26112417.log"
    path.write_bytes(b"17:52.299004-1,SDBL,3,Sdbl='" + b"x" * 2000 + b"'\n")
    loader = Mock()
    result = parse_file(path, "test", loader, 100, max_line_bytes=1024)
    assert result == (0, 0, None, False)
    loader.insert_batch.assert_not_called()


def test_stop_does_not_commit_partial_event(tmp_path):
    path = tmp_path / "26112417.log"
    path.write_bytes(b"17:52.299004-1,SDBL,3,Sdbl='begin\nmore\nend'\n")
    loader = Mock()
    stop = Mock(side_effect=[False, True])
    result = parse_file(path, "test", loader, 100, should_stop=stop)
    assert result == (0, 0, None, False)
    loader.insert_batch.assert_not_called()


def test_chunk_split_quotes():
    raw = "Context='а''б,в',Rows=17,Locks='foo''bar'"
    for size in range(1, len(raw) + 1):
        stream = StreamingProperties()
        for offset in range(0, len(raw), size):
            stream.feed(raw[offset:offset + size])
        assert stream.finish() == _parse_props(raw)


def test_long_physical_line(tmp_path):
    path = tmp_path / "26112417.log"
    with path.open("wb") as f:
        f.write(b"\xef\xbb\xbf17:52.299004-1,SDBL,3,Sdbl='")
        for _ in range(160):
            f.write("я".encode() * 32768)
        f.write(b"',Rows=17,Context='tail'\r\n")
        f.write(b"18:00.000001-1,CALL,1,Module=M,Method=F\n")
    loader = Mock()
    loader.insert_batch.return_value = True
    result = parse_file(path, "test", loader, 100, read_chunk_bytes=65537)
    assert result[:2] == (2, 2)
    rows = loader.insert_batch.call_args.args[0]
    assert rows[0]["sdbl"] == "я" * 100000
    assert rows[0]["rows"] == 17
    assert rows[0]["context"].startswith("tail")
    assert rows[1]["line_number"] == 2


def test_single_large_event(tmp_path):
    path = tmp_path / "26112417.log"
    path.write_bytes(b"17:52.299004-1,TLOCK,1,Locks=" + b"x" * 40000 +
                     b"\n18:00.000001-1,CALL,1,Module=M\n")
    loader = Mock()
    loader.insert_batch.return_value = True
    result = parse_file(path, "test", loader, 100, batch_max_bytes=100000,
                        single_event_max_bytes=1000000)
    assert result[:2] == (2, 2)
    assert len(loader.insert_batch.call_args_list) == 2
    assert loader.insert_batch.call_args_list[0].args[0][0]["locks"] == "x" * 40000


def test_single_event_hard_limit_reports_error(tmp_path):
    path = tmp_path / "26112417.log"
    path.write_bytes(b"17:52.299004-1,TLOCK,1,Locks=" + b"x" * 40000 + b"\n")
    loader = Mock()
    errors = []
    result = parse_file(path, "test", loader, 100, batch_max_bytes=100000,
                        single_event_max_bytes=200000, on_error=errors.append)
    assert result[:2] == (0, 0)
    assert len(errors) == 1
    loader.insert_batch.assert_not_called()
