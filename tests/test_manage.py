"""Tests for the single-machine run-management layer (bookdepth.manage).

Time is controlled through explicit ``now`` values and a FakeClock, and the
crash-recovery test uses a real subprocess that is SIGKILLed mid-run.
"""
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time

import pytest

from bookdepth import pipeline
from bookdepth.framework import Transport
from bookdepth.manage import ManageStore, Worker
from bookdepth.store import Store

ROOT = pathlib.Path(__file__).resolve().parents[1]


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = float(t)

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


@pytest.fixture()
def mstore(tmp_path):
    store = ManageStore(db_path=str(tmp_path / "manage.db"))
    yield store
    store.close()


@pytest.fixture()
def business():
    con = Store().connect()
    store = Store(con)
    store.reset()
    yield store
    store.reset()
    con.close()


# --------------------------------------------------------------------------- #
# Submission, querying, and mutual exclusion
# --------------------------------------------------------------------------- #
def test_manual_submit_and_status_query(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", trigger="manual", now=clock())
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "pending" and r["trigger_kind"] == "manual"
    assert r["can_continue"] is True and r["health"] == "waiting"
    assert r["next_attempt_at"] == clock()
    assert r["events"][0]["kind"] == "submitted"


def test_competing_claims_same_resource_are_serialized(mstore):
    clock = FakeClock()
    r1 = mstore.submit(mode="full", now=clock())
    r2 = mstore.submit(mode="incremental", now=clock())
    t1 = mstore.claim(r1, executor="e1", lease_s=30, now=clock())
    assert t1["ok"] and t1["epoch"] == 1
    # the second trigger for the same data resource is denied, with a reason
    t2 = mstore.claim(r2, executor="e2", lease_s=30, now=clock())
    assert not t2["ok"]
    assert "busy" in t2["denial"] and str(r1) in t2["denial"]
    r2d = mstore.get_run(r2, now=clock())
    assert "busy" in r2d["last_denial"]
    assert any(e["kind"] == "denied" for e in r2d["events"])
    # completing the holder releases the resource for the queued trigger
    fin = mstore.complete(r1, t1["epoch"], ok=True, result={"ingested": 1},
                          now=clock())
    assert fin["accepted"]
    t3 = mstore.claim(r2, executor="e2", lease_s=30, now=clock())
    assert t3["ok"]


def test_claim_next_skips_busy_resource_and_serves_oldest_due(mstore):
    clock = FakeClock()
    a1 = mstore.submit(resource="res-a", mode="full", now=clock())
    clock.advance(1)
    a2 = mstore.submit(resource="res-a", mode="full", now=clock())
    b1 = mstore.submit(resource="res-b", mode="full", now=clock())
    first = mstore.claim_next("w1", lease_s=30, now=clock())
    assert first["run_id"] == a1            # oldest due run goes first
    nxt = mstore.claim_next("w2", lease_s=30, now=clock())
    assert nxt["run_id"] == b1              # busy resource does not block others
    r_a2 = mstore.get_run(a2, now=clock())
    assert "busy" in (r_a2["last_denial"] or "")  # why it got no execution right
    assert mstore.claim_next("w3", lease_s=30, now=clock()) is None
    # releasing the resource lets the queued run go next (no starvation)
    mstore.complete(a1, first["epoch"], ok=True, result={}, now=clock())
    nxt2 = mstore.claim_next("w1", lease_s=30, now=clock())
    assert nxt2["run_id"] == a2


def test_state_survives_controller_replacement(tmp_path):
    db = str(tmp_path / "m.db")
    s1 = ManageStore(db_path=db)
    rid = s1.submit(mode="full", now=1000.0)
    s1.close()  # controller exits without any cleanup
    s2 = ManageStore(db_path=db)
    try:
        r = s2.get_run(rid, now=1000.0)
        assert r["status"] == "pending"
        t = s2.claim(rid, executor="new-controller", lease_s=10, now=1000.0)
        assert t["ok"]
    finally:
        s2.close()


# --------------------------------------------------------------------------- #
# Lease expiry, overdue messages, reappearing executors
# --------------------------------------------------------------------------- #
def test_lease_expiry_requeue_overdue_event_and_retry_window(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    t = mstore.claim(rid, executor="e1", lease_s=10, now=clock())
    assert t["ok"]
    clock.advance(11)
    # visible as non-continuable even before the reaper runs
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "running" and r["can_continue"] is False
    assert r["health"] == "lease_expired"
    assert mstore.reap(now=clock()) == [rid]
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "pending" and r["attempts"] == 1
    assert r["next_attempt_at"] == r["retry_at"] > clock()
    overdue = [e for e in r["events"] if e["kind"] == "expired"]
    assert overdue and overdue[0]["detail"]["executor"] == "e1"
    # the retry window is honoured before another attempt is allowed
    denied = mstore.claim(rid, executor="e2", lease_s=10, now=clock())
    assert not denied["ok"] and "not_eligible" in denied["denial"]
    assert mstore.claim_next("e2", lease_s=10, now=clock()) is None
    clock.advance(2.0)  # backoff for the first attempt is 1s with base 1
    t2 = mstore.claim(rid, executor="e2", lease_s=10, now=clock())
    # fencing epoch moved claim(1) -> reap(2) -> re-claim(3)
    assert t2["ok"] and t2["epoch"] == 3


def test_reappearing_executor_and_late_result_are_rejected(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    t1 = mstore.claim(rid, executor="ghost", lease_s=10, now=clock())
    clock.advance(15)
    mstore.reap(now=clock(), backoff_base=0.0)
    t2 = mstore.claim(rid, executor="living", lease_s=10, now=clock())
    assert t2["ok"]
    # the ghost comes back and tries to extend its lease and report results
    hb = mstore.heartbeat(rid, t1["epoch"], lease_s=10, now=clock())
    assert hb["ok"] is False
    late = mstore.complete(rid, t1["epoch"], ok=True, result={"ingested": 999},
                           now=clock())
    assert late["accepted"] is False
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "running" and r["epoch"] == t2["epoch"]
    assert any(e["kind"] == "stale_result" for e in r["events"])
    # the current executor's result is the one that lands
    fin = mstore.complete(rid, t2["epoch"], ok=True, result={"ingested": 3},
                          now=clock())
    assert fin["accepted"] is True
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "succeeded" and r["result"]["ingested"] == 3
    # a duplicate/late delivery after the terminal state is also rejected
    dup = mstore.complete(rid, t2["epoch"], ok=True, result={"ingested": 4},
                          now=clock())
    assert dup["accepted"] is False
    assert mstore.get_run(rid, now=clock())["result"]["ingested"] == 3


def test_heartbeat_extends_lease_and_detects_cancellation(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    t = mstore.claim(rid, executor="e1", lease_s=10, now=clock())
    clock.advance(8)
    ack = mstore.heartbeat(rid, t["epoch"], lease_s=10, now=clock())
    assert ack["ok"] and ack["lease_until"] == clock() + 10
    clock.advance(9)  # would have expired without the heartbeat
    assert mstore.reap(now=clock()) == []
    mstore.cancel(rid, now=clock())
    ack2 = mstore.heartbeat(rid, t["epoch"], lease_s=10, now=clock())
    assert ack2["ok"] is False and ack2["status"] == "cancelled"


# --------------------------------------------------------------------------- #
# Cancellation and its interleavings
# --------------------------------------------------------------------------- #
def test_cancel_pending_prevents_execution(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    res = mstore.cancel(rid, reason="no longer needed", now=clock())
    assert res["ok"]
    denied = mstore.claim(rid, executor="e1", lease_s=5, now=clock())
    assert not denied["ok"] and "cancelled" in denied["denial"]
    assert mstore.claim_next("e1", lease_s=5, now=clock()) is None
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "cancelled" and r["reason"] == "no longer needed"


def test_cancel_running_releases_resource_and_wins_against_late_complete(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    t = mstore.claim(rid, executor="e1", lease_s=30, now=clock())
    res = mstore.cancel(rid, reason="operator", now=clock())
    assert res["ok"] and res["status"] == "cancelled"
    # the resource is freed immediately for the next trigger
    rid2 = mstore.submit(mode="incremental", now=clock())
    t2 = mstore.claim(rid2, executor="e2", lease_s=30, now=clock())
    assert t2["ok"]
    # the cancelled executor's completion arrives late and is rejected
    late = mstore.complete(rid, t["epoch"], ok=True, result={"ingested": 1},
                           now=clock())
    assert late["accepted"] is False
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "cancelled" and r["reason"] == "operator"
    assert any(e["kind"] == "stale_result" for e in r["events"])


def test_complete_before_cancel_makes_cancel_a_noop(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    t = mstore.claim(rid, executor="e1", lease_s=30, now=clock())
    fin = mstore.complete(rid, t["epoch"], ok=True, result={"ingested": 2},
                          now=clock())
    assert fin["accepted"]
    res = mstore.cancel(rid, now=clock())
    assert res["ok"] is False and res["status"] == "succeeded"
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "succeeded"
    assert any(e["kind"] == "cancel_ignored" for e in r["events"])


# --------------------------------------------------------------------------- #
# Schedules
# --------------------------------------------------------------------------- #
def test_scheduled_tick_creates_due_runs_without_duplicates(mstore):
    clock = FakeClock()
    mstore.add_schedule("inc-5m", mode="incremental", interval_s=300,
                        start_at=clock() + 100, now=clock())
    assert mstore.tick(now=clock()) == []           # not due yet
    clock.advance(100)
    created = mstore.tick(now=clock())
    assert len(created) == 1
    r = mstore.get_run(created[0], now=clock())
    assert r["trigger_kind"] == "scheduled" and r["schedule_name"] == "inc-5m"
    assert r["due_at"] <= clock()
    assert mstore.tick(now=clock()) == []           # already fired this slot
    clock.advance(600)  # two intervals pass while the run is still queued
    assert mstore.tick(now=clock()) == []           # coalesced, no duplicates
    sch = [s for s in mstore.schedules() if s["name"] == "inc-5m"][0]
    assert sch["next_due_at"] > clock()


# --------------------------------------------------------------------------- #
# Worker behaviour, including with the real pipeline
# --------------------------------------------------------------------------- #
def test_failed_job_releases_resource_and_records_error(mstore):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())

    def boom(mode, should_stop):
        raise RuntimeError("source exploded")

    w = Worker(mstore, executor_id="w", lease_s=30, clock=clock, job_fn=boom)
    outcome = w.work_once()
    assert outcome["job_ok"] is False and outcome["recorded"] is True
    r = mstore.get_run(rid, now=clock())
    assert r["status"] == "failed" and "source exploded" in (r["reason"] or "")
    rid2 = mstore.submit(mode="full", now=clock())
    assert mstore.claim(rid2, executor="w", lease_s=30, now=clock())["ok"]


def test_worker_runs_real_pipeline_through_management(mstore, business):
    clock = FakeClock()
    rid = mstore.submit(mode="full", trigger="manual", now=clock())
    worker = Worker(
        mstore, executor_id="t1", lease_s=30, clock=clock,
        job_fn=lambda mode, stop: pipeline.run(mode=mode, con=business.con))
    outcome = worker.work_once()
    assert outcome["recorded"] and outcome["job_ok"]
    run = mstore.get_run(rid, now=clock())
    assert run["status"] == "succeeded"
    res = run["result"]
    assert res["engine"] == "crawl" and res["mode"] == "full"
    assert res["ingested"] >= 1
    # history records, quote computation and direction flags are intact
    recs = business.all_records()
    assert len(recs) == business.count() >= 1
    r0 = recs[0]
    assert r0["bid"] <= r0["ask"] and r0["valid_order"] == 1
    assert abs((r0["ask"] - r0["bid"]) - r0["spread"]) < 1e-6
    wm0 = business.watermark()
    assert wm0
    # an incremental managed run picks up new source rows and advances progress
    t = Transport()
    status, _h, _b = t.request("GET", "/refresh")
    assert status == 200
    rid2 = mstore.submit(mode="incremental", trigger="manual", now=clock())
    worker.work_once()
    run2 = mstore.get_run(rid2, now=clock())
    assert run2["status"] == "succeeded"
    assert run2["result"]["ingested"] >= 1
    assert business.watermark() > wm0


def test_cancel_does_not_retract_written_quotes(mstore, business):
    clock = FakeClock()
    rid = mstore.submit(mode="full", now=clock())
    ticket = mstore.claim(rid, executor="cancelled-soon", lease_s=30,
                          now=clock())
    assert ticket["ok"]
    written = threading.Event()

    def job(mode, should_stop):
        con = Store().connect()  # executors use their own business connection
        try:
            Store(con).upsert({"id": "q1", "ts": "2030-02-02T00:00:00Z",
                               "value": 5, "bid": 4.9, "ask": 5.1})
        finally:
            con.close()
        written.set()
        should_stop.wait(5.0)  # cooperative abort point
        return {"note": "stopped", "mode": mode}

    worker = Worker(mstore, executor_id="cancelled-soon", lease_s=30,
                    heartbeat_s=0.02, clock=clock, job_fn=job)
    box = {}
    th = threading.Thread(
        target=lambda: box.setdefault("o", worker.execute(ticket)))
    th.start()
    assert written.wait(5.0)
    res = mstore.cancel(rid, reason="operator stop", now=clock())
    assert res["ok"]
    th.join(timeout=10)
    outcome = box["o"]
    assert outcome["recorded"] is False  # late completion rejected
    run = mstore.get_run(rid, now=clock())
    assert run["status"] == "cancelled"
    assert any(e["kind"] == "stale_result" for e in run["events"])
    # the quote written before the cancel stays in the business tables
    assert business.count() == 1
    assert business.all_records()[0]["id"] == "q1"


# --------------------------------------------------------------------------- #
# Real interruption: a worker process is SIGKILLed mid-run
# --------------------------------------------------------------------------- #
def _subprocess_env():
    env = dict(os.environ)
    paths = [str(ROOT / "src"), str(ROOT)]
    if env.get("PYTHONPATH"):
        paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


def test_crashed_executor_lease_is_reclaimed_and_run_finishes(tmp_path):
    db = str(tmp_path / "manage.db")
    store = ManageStore(db_path=db)
    run_id = store.submit(mode="full", trigger="manual")
    proc = subprocess.Popen(
        [sys.executable, "-m", "bookdepth.manage", "--db", db, "work",
         "--once", "--lease", "0.5", "--sleep", "30", "--executor", "doomed"],
        cwd=str(ROOT), env=_subprocess_env(),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.time() + 15
        while True:
            run = store.get_run(run_id)
            if run and run["status"] == "running":
                break
            if time.time() > deadline:
                raise AssertionError("doomed worker never claimed the run")
            time.sleep(0.05)
        proc.send_signal(signal.SIGKILL)  # real interruption, no cleanup
        proc.wait(timeout=15)
        run = store.get_run(run_id)
        assert run["status"] == "running"  # nobody cleaned up after the crash
        lease_until = run["lease_until"]
        wait = lease_until - time.time()
        if wait > 0:
            time.sleep(wait + 0.1)
        run = store.get_run(run_id)
        assert run["can_continue"] is False  # lease is over
        assert store.reap(backoff_base=0.0) == [run_id]
        run = store.get_run(run_id)
        assert run["status"] == "pending"
        assert any(e["kind"] == "expired" for e in run["events"])
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    store.close()

    out = subprocess.run(
        [sys.executable, "-m", "bookdepth.manage", "--db", db, "work",
         "--once", "--lease", "5", "--sleep", "0.1", "--executor", "survivor"],
        cwd=str(ROOT), env=_subprocess_env(), capture_output=True, text=True,
        timeout=60)
    assert out.returncode == 0, out.stderr
    store = ManageStore(db_path=db)
    try:
        run = store.get_run(run_id)
        assert run["status"] == "succeeded"
        assert run["attempts"] == 2
        kinds = [e["kind"] for e in run["events"]]
        assert kinds.count("claimed") == 2
        assert "expired" in kinds and "completed" in kinds
    finally:
        store.close()
