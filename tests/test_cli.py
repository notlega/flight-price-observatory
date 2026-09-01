import argparse
import logging
import sys
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import cli.__main__ as mainmod
from cli.convert import run as convert_run
from cli.search import _ahead_days, _async_run
from cli.search import run as search_run


def test_main_requires_subcommand(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["obs"])
    with pytest.raises(SystemExit):
        mainmod.main()


def test_main_dispatch_routes_to_search_run(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("cli.search.run", fake)
    monkeypatch.setattr(
        sys,
        "argv",
        ["obs", "search", "--start", "2026-08-01", "--max-days", "3"],
    )
    mainmod.main()
    assert fake.called
    args = fake.call_args.args[0]
    assert args.start == date(2026, 8, 1)
    assert args.max_days == 3
    assert not hasattr(args, "keep_db")
    assert args.func is fake


def test_main_dispatch_routes_to_convert_run(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("cli.convert.run", fake)
    monkeypatch.setattr(sys, "argv", ["obs", "convert", "x.db"])
    mainmod.main()
    assert fake.called
    args = fake.call_args.args[0]
    assert args.db == "x.db"
    assert args.output is None
    assert not hasattr(args, "keep_db")


def test_main_verbose_sets_debug_logging(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("cli.search.run", fake)
    monkeypatch.setattr(sys, "argv", ["obs", "-v", "search"])
    with patch.object(logging, "basicConfig") as bc:
        mainmod.main()
    assert bc.call_args.kwargs["level"] == logging.DEBUG


def test_main_default_logging_is_info(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("cli.search.run", fake)
    monkeypatch.setattr(sys, "argv", ["obs", "search"])
    with patch.object(logging, "basicConfig") as bc:
        mainmod.main()
    assert bc.call_args.kwargs["level"] == logging.INFO


def test_search_run_wraps_async(monkeypatch):
    fake = AsyncMock()
    monkeypatch.setattr("cli.search._async_run", fake)
    search_run(
        argparse.Namespace(
            start=None,
            max_days=None,
            currency=None,
            rate=None,
            workers=None,
            continue_run=False,
        )
    )
    fake.assert_awaited_once()


def test_search_run_repository_state_error_exits_1(monkeypatch, capsys):
    from collector.errors import RepositoryStateError

    async def boom(*args, **kwargs):
        raise RepositoryStateError("no existing database")

    monkeypatch.setattr("cli.search._async_run", boom)
    with pytest.raises(SystemExit) as exc:
        search_run(argparse.Namespace())
    assert exc.value.code == 1
    assert "error: no existing database" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_search_async_run_forwards_args():
    manager = MagicMock()
    manager.run = AsyncMock()
    start = date(2026, 8, 1)
    args = argparse.Namespace(
        start=start,
        max_days=3,
        currency="USD",
        rate=10.0,
        workers=5,
        continue_run=True,
    )
    with (
        patch("cli.search.CollectorManager", return_value=manager),
        patch("cli.search.ProviderRegistry") as registry,
    ):
        await _async_run(args)
    end = start + timedelta(days=3)
    manager.run.assert_awaited_once_with(
        start_date=start,
        end_date=end,
        max_days_ahead=_ahead_days(end),
        currency="USD",
        rate=10.0,
        workers=5,
        continue_run=True,
    )
    registry.assert_called_once()


def test_convert_run_never_deletes_db(monkeypatch, capsys):
    convert_fn = AsyncMock(return_value="/tmp/out.jsonl")
    monkeypatch.setattr("cli.convert.convert", convert_fn)
    convert_run(argparse.Namespace(db="s.db", output="o.jsonl"))
    convert_fn.assert_awaited_once_with("s.db", "o.jsonl")
    assert "Output: /tmp/out.jsonl" in capsys.readouterr().out


def test_ahead_days_pins_zero_for_past_end():
    assert _ahead_days(date.today() - timedelta(days=1)) == 0


def test_main_dispatch_routes_to_transform_run(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("cli.transform.run", fake)
    monkeypatch.setattr(
        sys,
        "argv",
        ["obs", "transform", "--input", "in.jsonl", "--output", "out"],
    )
    mainmod.main()
    assert fake.called
    args = fake.call_args.args[0]
    assert args.input == "in.jsonl"
    assert args.output == "out"


def test_main_dispatch_routes_to_publish_run(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr("cli.publish.run", fake)
    monkeypatch.setattr(sys, "argv", ["obs", "publish"])
    mainmod.main()
    assert fake.called
    args = fake.call_args.args[0]
    assert args.input == "storage/silver"


def test_transform_run_writes_partitioned_parquet(monkeypatch, tmp_path, capsys):
    import cli.transform as transform_mod

    executed: list[str] = []

    class FakeDuckDB:
        def execute(self, sql: str):
            executed.append(sql)

            class Result:
                def fetchone(self):
                    return (7,)

                def fetchall(self):
                    return []

            return Result()

        def close(self):
            pass

    monkeypatch.setattr("duckdb.connect", lambda *a, **k: FakeDuckDB())
    transform_mod.run(argparse.Namespace(input="in.jsonl", output="out"))
    assert len(executed) == 3
    assert "read_json_auto('in.jsonl'" in executed[0]
    assert any("PARTITION_BY (origin, destination)" in e for e in executed)
    assert "Output: out" in capsys.readouterr().out


def test_publish_uploads_parquet_tree(monkeypatch, tmp_path, capsys):
    import cli.publish as publish_mod

    (tmp_path / "a.parquet").write_text("x")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.parquet").write_text("y")

    uploaded: list[tuple[str, str, str]] = []

    class FakeClient:
        def upload_file(self, path, bucket, key):
            uploaded.append((path, bucket, key))

    monkeypatch.setattr(publish_mod, "_r2_client", lambda: FakeClient())
    monkeypatch.setenv("R2_BUCKET", "bucket")
    publish_mod.run(argparse.Namespace(input=str(tmp_path)))
    assert len(uploaded) == 2
    assert uploaded[0][1] == "bucket"
    assert uploaded[0][2] == "silver/a.parquet"
    assert uploaded[1][2] == "silver/sub/b.parquet"
    assert "Uploaded 2 Parquet files" in capsys.readouterr().out


def test_publish_missing_env_exits(monkeypatch):
    import cli.publish as publish_mod

    for key in (
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(SystemExit) as exc:
        publish_mod._r2_client()
    assert exc.value.code != 0
