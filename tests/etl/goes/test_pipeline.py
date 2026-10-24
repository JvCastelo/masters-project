"""Tests for masters_project.etl.goes.pipeline: planning, collecting with a pool, and exporting."""

import re
import runpy
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import nullcontext
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from masters_project.etl.goes import pipeline
from masters_project.etl.goes.cache import (
    extractions_to_frame,
    load_day,
    save_day,
)
from masters_project.etl.goes.reader import GoesProcessor
from masters_project.etl.goes.worker import ExtractionJob, FileExtraction
from masters_project.file_paths import file_paths
from masters_project.settings import StationSettings, settings

CHANNEL = "C03"
STATION = "OURINHOS"
PIXELS = {STATION: (10, 20)}
COLUMNS = GoesProcessor.window_columns(1, CHANNEL)
DAY = (2022, 172)  # 2022-06-21


def _key(minute: int, hour: int = 15, channel: str = CHANNEL) -> str:
    return (
        f"noaa-goes16/ABI-L1b-RadF/2022/172/{hour:02d}/"
        f"OR_ABI-L1b-RadF-M6{channel}_G16_s2022172{hour:02d}{minute:02d}000_e0_c0.nc"
    )


def fake_extract(path: str, job: ExtractionJob) -> FileExtraction:
    """Stand-in for the process-pool worker: no S3, windows derived from the scan minute."""
    scan = re.search(r"_s2022172(\d{2})(\d{2})", path)
    hour, minute = int(scan.group(1)), int(scan.group(2))
    if minute == 59:
        return FileExtraction(source_key=path, error="OSError: truncated file")
    stamp = pd.Timestamp(f"2022-06-21 {hour:02d}:{minute:02d}", tz="UTC")
    windows = {
        station: np.full(9, float(minute + index))
        for index, station in enumerate(job.pixels)
    }
    return FileExtraction(source_key=path, timestamp=stamp, windows=windows)


@pytest.fixture
def goes_env(data_tree, one_station, monkeypatch: pytest.MonkeyPatch):
    """One station, one channel, one day; the pool runs in threads and extraction is faked."""
    goes = settings.etl.goes
    monkeypatch.setattr(goes, "channels", [CHANNEL])
    monkeypatch.setattr(goes, "pixel_radius", 1)
    monkeypatch.setattr(goes, "skip_days_without_target", False)
    monkeypatch.setattr(pipeline, "extract_file", fake_extract)
    monkeypatch.setattr(
        pipeline,
        "ProcessPoolExecutor",
        lambda max_workers, max_tasks_per_child: ThreadPoolExecutor(max_workers=2),
    )
    monkeypatch.setattr(
        pipeline, "locate_pixels", lambda client, first_file: dict(PIXELS)
    )


def _plan(files: list[str]) -> pipeline.ChannelPlan:
    return pipeline.plan_channel(MagicMock(), CHANNEL, files)


# =============================================================================
# pixels
# =============================================================================


def test_locate_pixels_computes_one_pixel_per_station(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        settings.etl,
        "stations",
        {
            "A": StationSettings(latitude=-20.0, longitude=-40.0),
            "B": StationSettings(latitude=-30.0, longitude=-50.0),
        },
    )
    monkeypatch.setattr(
        GoesProcessor, "open_as_dataset", lambda file: nullcontext("ds")
    )
    monkeypatch.setattr(
        GoesProcessor,
        "get_target_indices",
        lambda ds, lat, lon: (int(-lat), int(-lon)),
    )
    client = MagicMock()
    client.open_partial.return_value = nullcontext("file")

    pixels = pipeline.locate_pixels(client, "first.nc")

    assert pixels == {"A": (20, 40), "B": (30, 50)}
    client.open_partial.assert_called_once_with("first.nc")


def test_resolve_pixels_computes_once_and_then_reads_the_cache(
    data_tree, one_station, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def locate(client, first_file):
        calls.append(first_file)
        return dict(PIXELS)

    monkeypatch.setattr(pipeline, "locate_pixels", locate)

    first = pipeline.resolve_pixels(MagicMock(), CHANNEL, "a.nc")
    second = pipeline.resolve_pixels(MagicMock(), CHANNEL, "b.nc")

    assert first == second == PIXELS
    assert calls == ["a.nc"]


def test_resolve_pixels_recomputes_when_a_station_moves(
    data_tree, one_station, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []
    monkeypatch.setattr(
        pipeline,
        "locate_pixels",
        lambda client, first_file: calls.append(1) or dict(PIXELS),
    )

    pipeline.resolve_pixels(MagicMock(), CHANNEL, "a.nc")
    monkeypatch.setattr(
        settings.etl,
        "stations",
        {STATION: StationSettings(latitude=-23.5, longitude=-49.0)},
    )
    pipeline.resolve_pixels(MagicMock(), CHANNEL, "a.nc")

    assert len(calls) == 2


# =============================================================================
# planning
# =============================================================================


def test_plan_channel_marks_every_file_pending_on_a_cold_cache(goes_env) -> None:
    files = [_key(0), _key(10), _key(20)]
    plan = _plan(files)

    assert plan.pixels == PIXELS
    assert plan.pending == {DAY: files}


def test_plan_channel_skips_files_already_in_the_day_cache(goes_env) -> None:
    files = [_key(0), _key(10), _key(20)]
    plan = _plan(files)
    done = extractions_to_frame(
        [fake_extract(f, MagicMock(pixels=PIXELS)) for f in files[:2]], PIXELS, COLUMNS
    )
    save_day(plan.cache_paths[DAY], plan.cached[DAY], done)

    replanned = _plan(files)

    assert replanned.pending == {DAY: [files[2]]}


def test_iter_tasks_yields_only_pending_files_with_their_job(goes_env) -> None:
    files = [_key(0), _key(10)]
    plan = _plan(files)

    tasks = list(pipeline.iter_tasks([plan]))

    assert [path for path, _, _ in tasks] == files
    path, job, key = tasks[0]
    assert key == (CHANNEL, DAY)
    assert job.pixels == PIXELS and job.radius == 1 and job.variable == "Rad"


# =============================================================================
# running the pool
# =============================================================================


def test_run_bounded_delivers_every_result_with_a_small_window(goes_env) -> None:
    files = [_key(m) for m in range(0, 50, 10)]  # 5 tasks, window of 2
    job = ExtractionJob("ABI-L1b-RadF", "noaa-goes16", "Rad", 1, PIXELS)
    tasks = iter((path, job, (CHANNEL, DAY)) for path in files)
    received = []

    with ThreadPoolExecutor(max_workers=2) as pool:
        pipeline.run_bounded(
            pool,
            tasks,
            lambda key, item: received.append((key, item.source_key)),
            window=2,
        )

    assert sorted(path for _, path in received) == sorted(files)
    assert {key for key, _ in received} == {(CHANNEL, DAY)}


def test_run_bounded_never_has_more_tasks_in_flight_than_the_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingPool:
        def __init__(self) -> None:
            self.open = 0
            self.peak = 0

        def submit(self, fn, *args):
            self.open += 1
            self.peak = max(self.peak, self.open)
            future: Future = Future()
            future.set_result(fn(*args))
            return future

    pool = RecordingPool()
    delivered = []

    def on_result(key, item) -> None:
        pool.open -= 1
        delivered.append(item)

    monkeypatch.setattr(pipeline, "extract_file", lambda path, job: path)
    tasks = iter((str(n), None, "key") for n in range(10))

    pipeline.run_bounded(pool, tasks, on_result, window=3)

    assert pool.peak <= 3
    assert len(delivered) == 10


def test_collect_saves_each_day_to_the_cache(goes_env) -> None:
    files = [_key(0), _key(10), _key(20)]
    plan = _plan(files)

    pipeline.collect([plan])

    cached = load_day(plan.cache_paths[DAY], COLUMNS)
    assert sorted(cached["source_key"]) == sorted(files)
    assert len(plan.cached[DAY]) == 3  # the in-memory plan is updated too


def test_collect_keeps_the_good_files_and_retries_the_failed_one(
    goes_env, caplog: pytest.LogCaptureFixture
) -> None:
    """Minute 59 is a truncated file: logged, left out of the cache, pending next run."""
    files = [_key(0), _key(59)]
    plan = _plan(files)

    pipeline.collect([plan])

    assert "Failed to process" in caplog.text and "1 files failed" in caplog.text
    assert _plan(files).pending == {DAY: [_key(59)]}


def test_collect_without_pending_files_does_not_start_a_pool(
    goes_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = [_key(0)]
    plan = _plan(files)
    pipeline.collect([plan])

    def no_pool(**kwargs):
        raise AssertionError("pool started for nothing")

    monkeypatch.setattr(pipeline, "ProcessPoolExecutor", no_pool)
    pipeline.collect([_plan(files)])  # everything cached now


# =============================================================================
# export
# =============================================================================


def test_export_channel_writes_one_sorted_csv_per_station(goes_env) -> None:
    files = [_key(20), _key(0), _key(10)]
    plan = _plan(files)
    pipeline.collect([plan])

    pipeline.export_channel(plan)

    out = pd.read_csv(file_paths.raw_goes(CHANNEL, STATION), parse_dates=["timestamp"])
    assert out["timestamp"].is_monotonic_increasing
    assert list(out.columns) == [*COLUMNS, "timestamp"]
    assert len(out) == 3


def test_export_channel_leaves_out_rows_cached_for_other_periods(goes_env) -> None:
    plan = _plan([_key(0), _key(10)])
    pipeline.collect([plan])
    narrower = _plan([_key(0)])  # this run only asks for one of the cached scans

    pipeline.export_channel(narrower)

    assert len(pd.read_csv(file_paths.raw_goes(CHANNEL, STATION))) == 1


def test_export_channel_with_no_data_warns_and_writes_nothing(
    goes_env, caplog: pytest.LogCaptureFixture
) -> None:
    plan = _plan([_key(59)])  # the only file fails
    pipeline.collect([plan])

    pipeline.export_channel(plan)

    assert "No valid data extracted" in caplog.text
    assert not file_paths.raw_goes(CHANNEL, STATION).exists()


def test_export_channel_skips_a_station_without_rows(
    goes_env, caplog: pytest.LogCaptureFixture
) -> None:
    plan = _plan([_key(0)])
    pipeline.collect([plan])
    plan.pixels = {STATION: (10, 20), "GHOST": (1, 1)}

    with caplog.at_level("WARNING"):
        pipeline.export_channel(plan)

    assert "No valid data for GHOST" in caplog.text
    assert file_paths.raw_goes(CHANNEL, STATION).exists()


# =============================================================================
# target days
# =============================================================================


def test_target_days_is_none_when_a_sonda_csv_is_missing(
    data_tree, one_station, caplog: pytest.LogCaptureFixture
) -> None:
    assert pipeline.target_days() is None
    assert "SONDA CSV missing" in caplog.text


def test_target_days_lists_the_days_with_a_valid_target(data_tree, one_station) -> None:
    path = file_paths.raw_sonda(STATION)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": ["2022-06-21 12:00:00+00:00", "2022-06-22 12:00:00+00:00"],
            "glo_avg": [500.0, np.nan],
        }
    ).to_csv(path, index=False)

    days = pipeline.target_days()

    assert days == {pd.Timestamp("2022-06-21").date()}


# =============================================================================
# main
# =============================================================================


class FakeClient:
    """GoesS3Client stand-in: lists three scans for C03 and nothing for C13."""

    last: "FakeClient"

    def __init__(self, product_name: str, bucket_name: str) -> None:
        FakeClient.last = self
        self.calls = 0

    def list_files_by_channel(self, channels, **kwargs):
        self.calls += 1
        return {
            c: ([_key(0), _key(10), _key(20)] if c == CHANNEL else []) for c in channels
        }


def test_main_collects_and_exports_every_channel_that_has_files(
    goes_env, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(pipeline, "GoesS3Client", FakeClient)
    monkeypatch.setattr(settings.etl.goes, "channels", [CHANNEL, "C13"])

    pipeline.main()

    out = pd.read_csv(file_paths.raw_goes(CHANNEL, STATION))
    assert len(out) == 3
    assert not file_paths.raw_goes("C13", STATION).exists()
    assert "[C13] No files found" in caplog.text


def test_main_second_run_is_served_from_the_cache(
    goes_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pipeline, "GoesS3Client", FakeClient)
    pipeline.main()

    def must_not_run(path, job):
        raise AssertionError("a cached file was read again")

    monkeypatch.setattr(pipeline, "extract_file", must_not_run)
    pipeline.main()  # would raise inside the pool if anything were pending

    assert len(pd.read_csv(file_paths.raw_goes(CHANNEL, STATION))) == 3


def test_main_skips_a_channel_that_cannot_be_planned(
    goes_env, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(pipeline, "GoesS3Client", FakeClient)

    def broken(client, channel, first_file):
        raise RuntimeError("no grid")

    monkeypatch.setattr(pipeline, "resolve_pixels", broken)

    pipeline.main()

    assert "Could not plan this channel" in caplog.text
    assert not file_paths.raw_goes(CHANNEL, STATION).exists()


def test_main_restricts_listing_to_days_with_a_target(
    goes_env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.etl.goes, "skip_days_without_target", True)
    captured = {}

    class Client(FakeClient):
        def list_files_by_channel(self, channels, **kwargs):
            captured.update(kwargs)
            return {c: [] for c in channels}

    monkeypatch.setattr(pipeline, "GoesS3Client", Client)
    day = pd.Timestamp("2022-06-21").date()
    monkeypatch.setattr(pipeline, "target_days", lambda: {day})

    pipeline.main()

    assert captured["only_days"] == {day}


def test_module_entry_point_runs_main(monkeypatch: pytest.MonkeyPatch) -> None:
    called = []
    monkeypatch.setattr(pipeline, "main", lambda: called.append(True))

    runpy.run_module("masters_project.etl.goes", run_name="__main__")

    assert called == [True]
