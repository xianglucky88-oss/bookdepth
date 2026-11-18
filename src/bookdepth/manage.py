"""Single-machine run management for the collection pipeline.

The existing manual commands (``python -m bookdepth.cli``, ``make run``) keep
working untouched; this module adds a management entry on top of the same
pipeline so that scheduled due tasks and manual catch-up runs can be
submitted, claimed, cancelled and inspected on one machine:

* ``submit`` registers a manual run; ``add_schedule`` + ``tick`` create runs
  for recurring tasks as they fall due.
* ``claim`` / ``claim_next`` hand out execution rights together with a lease
  and a fencing epoch: one data resource is never occupied in parallel, the
  oldest due run goes first, and a busy resource never blocks runs for other
  resources (no starvation).
* ``heartbeat`` / ``complete`` let an executor prove liveness and record the
  final result; ``cancel`` fences off the current executor immediately.
* ``reap`` requeues runs whose lease expired (executor lost) and records an
  overdue event; late results from stale epochs are rejected, and
  cancel/complete races resolve to whichever transaction committed first, so
  the stored record stays trustworthy when the controlling process exits, an
  executor disappears and reappears, or messages arrive late.

``get_run`` / ``list_runs`` expose whether an unfinished run can still
continue, when it can be attempted again, and why it did not get execution
rights (``last_denial`` plus the event log).

Cancelling a run never retracts quotes already written to the business
tables; it only settles how the run itself is recorded.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from .config import DB_PATH, SPEC

TERMINAL = ("succeeded", "failed", "cancelled")
MODES = ("full", "incremental")
DEFAULT_RESOURCE = SPEC.slug

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  resource TEXT NOT NULL,
  mode TEXT NOT NULL,
  trigger_kind TEXT NOT NULL,
  schedule_name TEXT,
  status TEXT NOT NULL,
  epoch INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  due_at REAL NOT NULL,
  retry_at REAL,
  claimed_at REAL,
  lease_until REAL,
  heartbeat_at REAL,
  finished_at REAL,
  executor TEXT,
  result TEXT,
  reason TEXT,
  last_denial TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_status_due ON runs(status, due_at);
CREATE TABLE IF NOT EXISTS resource_locks(
  resource TEXT PRIMARY KEY,
  run_id INTEGER,
  epoch INTEGER NOT NULL DEFAULT 0,
  lease_until REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS schedules(
  name TEXT PRIMARY KEY,
  resource TEXT NOT NULL,
  mode TEXT NOT NULL,
  interval_s REAL NOT NULL,
  next_due_at REAL NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS run_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT
);
"""


def _dump(obj):
    return json.dumps(obj, ensure_ascii=False, default=repr)


def _now():
    return time.time()


class ManageStore:
    """Durable, transactional home for runs, leases, schedules and events.

    Every mutating operation is a single ``BEGIN IMMEDIATE`` transaction, so
    concurrent CLI processes on one machine serialise through SQLite and a
    crashed process never leaves a half-written record.  The store keeps no
    process-local state: any controller can exit and another can take over
    using only what is in the database.
    """

    def __init__(self, db_path=None):
        self.path = str(db_path or DB_PATH)
        self._lock = threading.RLock()
        self.con = sqlite3.connect(self.path, check_same_thread=False,
                                   isolation_level=None)
        self.con.row_factory = sqlite3.Row
        with self._lock:
            self.con.execute("PRAGMA busy_timeout=5000")
            try:
                self.con.execute("PRAGMA journal_mode=WAL")
            except sqlite3.DatabaseError:
                pass
            self.con.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self.con.close()

    @contextmanager
    def _tx(self):
        with self._lock:
            self.con.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self.con.execute("ROLLBACK")
                raise
            self.con.execute("COMMIT")

    def _event(self, run_id, kind, ts, detail=None):
        self.con.execute(
            "INSERT INTO run_events(run_id,ts,kind,detail) VALUES(?,?,?,?)",
            (run_id, ts, kind, _dump(detail) if detail is not None else None))

    # ------------------------------------------------------------------ #
    # Inspection
    # ------------------------------------------------------------------ #
    def _run_row(self, run_id):
        return self.con.execute("SELECT * FROM runs WHERE id=?",
                                (run_id,)).fetchone()

    def _lock_row(self, resource):
        return self.con.execute("SELECT * FROM resource_locks WHERE resource=?",
                                (resource,)).fetchone()

    def _describe(self, row, now):
        d = dict(row)
        d["result"] = json.loads(d["result"]) if d["result"] else None
        status = d["status"]
        if status == "running":
            alive = (d["lease_until"] or 0) > now
            d["can_continue"] = alive
            d["health"] = "ok" if alive else "lease_expired"
            d["next_attempt_at"] = None
            d["hint"] = ("executing; lease valid until %.3f" % d["lease_until"]
                         if alive else
                         "lease expired; executor presumed lost; "
                         "reap will requeue it")
        elif status == "pending":
            nxt = max(d["due_at"] or 0, d["retry_at"] or 0)
            d["can_continue"] = True
            d["health"] = "waiting"
            d["next_attempt_at"] = nxt
            d["hint"] = "queued; eligible at %.3f" % nxt
        else:
            d["can_continue"] = False
            d["health"] = "terminal"
            d["next_attempt_at"] = None
            d["hint"] = "finished: %s" % status
        return d

    def get_run(self, run_id, now=None):
        """Full picture of one run: state, timing, denial reason, events."""
        now = _now() if now is None else now
        with self._lock:
            row = self._run_row(run_id)
            if row is None:
                return None
            d = self._describe(row, now)
            d["events"] = [
                {"ts": e["ts"], "kind": e["kind"],
                 "detail": json.loads(e["detail"]) if e["detail"] else None}
                for e in self.con.execute(
                    "SELECT ts,kind,detail FROM run_events WHERE run_id=?"
                    " ORDER BY id", (run_id,))]
            return d

    def list_runs(self, now=None, limit=100):
        now = _now() if now is None else now
        with self._lock:
            rows = self.con.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT ?",
                (limit,)).fetchall()
            return [self._describe(r, now) for r in rows]

    # ------------------------------------------------------------------ #
    # Submission and scheduling
    # ------------------------------------------------------------------ #
    def submit(self, resource=DEFAULT_RESOURCE, mode="full", trigger="manual",
               schedule_name=None, due_at=None, now=None):
        """Register a run; manual catch-up runs are due immediately."""
        if mode not in MODES:
            raise ValueError("unknown mode: %r" % (mode,))
        now = _now() if now is None else now
        due = now if due_at is None else float(due_at)
        with self._tx():
            cur = self.con.execute(
                "INSERT INTO runs(resource,mode,trigger_kind,schedule_name,"
                "status,created_at,due_at) VALUES(?,?,?,?,'pending',?,?)",
                (resource, mode, trigger, schedule_name, now, due))
            run_id = cur.lastrowid
            self._event(run_id, "submitted", now,
                        {"resource": resource, "mode": mode, "trigger": trigger,
                         "schedule": schedule_name, "due_at": due})
        return run_id

    def add_schedule(self, name, mode="incremental", interval_s=300.0,
                     resource=DEFAULT_RESOURCE, start_at=None, enabled=True,
                     now=None):
        if mode not in MODES:
            raise ValueError("unknown mode: %r" % (mode,))
        now = _now() if now is None else now
        nxt = float(start_at) if start_at is not None else now + float(interval_s)
        with self._tx():
            self.con.execute(
                "INSERT INTO schedules(name,resource,mode,interval_s,"
                "next_due_at,enabled) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET resource=excluded.resource,"
                " mode=excluded.mode, interval_s=excluded.interval_s,"
                " next_due_at=excluded.next_due_at, enabled=excluded.enabled",
                (name, resource, mode, float(interval_s), nxt,
                 1 if enabled else 0))
        return name

    def remove_schedule(self, name):
        with self._tx():
            cur = self.con.execute("DELETE FROM schedules WHERE name=?", (name,))
            return cur.rowcount > 0

    def schedules(self):
        with self._lock:
            return [dict(r) for r in
                    self.con.execute("SELECT * FROM schedules ORDER BY name")]

    def tick(self, now=None):
        """Create runs for schedules that fell due.

        At most one active (pending/running) run per schedule is kept, and
        missed intervals are collapsed, so a controller that was offline for
        a while does not flood the queue when it comes back.
        """
        now = _now() if now is None else now
        created = []
        with self._tx():
            due = self.con.execute(
                "SELECT * FROM schedules WHERE enabled=1 AND next_due_at <= ?"
                " ORDER BY next_due_at, name", (now,)).fetchall()
            for s in due:
                active = self.con.execute(
                    "SELECT COUNT(*) AS c FROM runs WHERE schedule_name=?"
                    " AND status IN ('pending','running')",
                    (s["name"],)).fetchone()["c"]
                if not active:
                    cur = self.con.execute(
                        "INSERT INTO runs(resource,mode,trigger_kind,"
                        "schedule_name,status,created_at,due_at)"
                        " VALUES(?,?,'scheduled',?,'pending',?,?)",
                        (s["resource"], s["mode"], s["name"], now,
                         s["next_due_at"]))
                    run_id = cur.lastrowid
                    self._event(run_id, "submitted", now,
                                {"resource": s["resource"], "mode": s["mode"],
                                 "trigger": "scheduled", "schedule": s["name"],
                                 "due_at": s["next_due_at"]})
                    created.append(run_id)
                nxt = s["next_due_at"]
                while nxt <= now:
                    nxt += s["interval_s"]
                self.con.execute(
                    "UPDATE schedules SET next_due_at=? WHERE name=?",
                    (nxt, s["name"]))
        return created

    # ------------------------------------------------------------------ #
    # Claiming: mutual exclusion per resource, fair across runs
    # ------------------------------------------------------------------ #
    def _check_claimable(self, row, now):
        status = row["status"]
        if status != "pending":
            return "terminal_or_active: run %s is %s" % (row["id"], status)
        eligible = max(row["due_at"] or 0, row["retry_at"] or 0)
        if eligible > now:
            return ("not_eligible: run %s can be attempted at %.3f (in %.3fs)"
                    % (row["id"], eligible, eligible - now))
        lock = self._lock_row(row["resource"])
        if lock and lock["lease_until"] > now:
            return ("resource_busy: resource '%s' held by run %s until %.3f"
                    " (in %.3fs)" % (row["resource"], lock["run_id"],
                                     lock["lease_until"],
                                     lock["lease_until"] - now))
        return None

    def _record_denial(self, row, denial, now):
        prev_kind = (row["last_denial"] or "").split(":", 1)[0]
        self.con.execute("UPDATE runs SET last_denial=? WHERE id=?",
                         (denial, row["id"]))
        if prev_kind != denial.split(":", 1)[0]:
            self._event(row["id"], "denied", now, {"reason": denial})

    def _do_claim(self, row, executor, lease_s, now):
        epoch = row["epoch"] + 1
        lease_until = now + lease_s
        self.con.execute(
            "UPDATE runs SET status='running', epoch=?, attempts=attempts+1,"
            " claimed_at=?, lease_until=?, heartbeat_at=?, executor=?,"
            " last_denial=NULL WHERE id=?",
            (epoch, now, lease_until, now, executor, row["id"]))
        self.con.execute(
            "INSERT INTO resource_locks(resource,run_id,epoch,lease_until)"
            " VALUES(?,?,?,?) ON CONFLICT(resource) DO UPDATE SET"
            " run_id=excluded.run_id, epoch=excluded.epoch,"
            " lease_until=excluded.lease_until",
            (row["resource"], row["id"], epoch, lease_until))
        self._event(row["id"], "claimed", now,
                    {"executor": executor, "epoch": epoch,
                     "lease_until": lease_until,
                     "attempt": row["attempts"] + 1})
        return {"ok": True, "run_id": row["id"], "resource": row["resource"],
                "mode": row["mode"], "epoch": epoch, "lease_until": lease_until,
                "executor": executor}

    def claim(self, run_id, executor, lease_s=30.0, now=None):
        """Try to claim one specific run; records why it was denied."""
        now = _now() if now is None else now
        with self._tx():
            row = self._run_row(run_id)
            if row is None:
                return {"ok": False, "run_id": run_id,
                        "denial": "missing: no such run"}
            denial = self._check_claimable(row, now)
            if denial:
                self._record_denial(row, denial, now)
                return {"ok": False, "run_id": run_id, "denial": denial}
            return self._do_claim(row, executor, lease_s, now)

    def claim_next(self, executor, lease_s=30.0, now=None, resources=None):
        """Claim the oldest due run whose resource is free.

        Runs are considered in ``(due_at, id)`` order, so the oldest waiting
        trigger is served first and no run is starved; a run whose resource is
        leased is skipped (with its denial recorded) rather than blocking
        runs for other resources.
        """
        now = _now() if now is None else now
        with self._tx():
            rows = self.con.execute(
                "SELECT * FROM runs WHERE status='pending'"
                " ORDER BY due_at, id").fetchall()
            for row in rows:
                if resources and row["resource"] not in resources:
                    continue
                denial = self._check_claimable(row, now)
                if denial:
                    self._record_denial(row, denial, now)
                    continue
                return self._do_claim(row, executor, lease_s, now)
            return None

    # ------------------------------------------------------------------ #
    # Executor liveness and outcomes
    # ------------------------------------------------------------------ #
    def heartbeat(self, run_id, epoch, lease_s=30.0, now=None):
        """Extend the lease; fails once the run was cancelled or re-claimed."""
        now = _now() if now is None else now
        with self._tx():
            row = self._run_row(run_id)
            if row is None:
                return {"ok": False, "status": "missing"}
            if row["status"] != "running" or row["epoch"] != epoch:
                return {"ok": False, "status": row["status"],
                        "epoch": row["epoch"],
                        "detail": "stale epoch or no longer running"}
            lease_until = now + lease_s
            self.con.execute(
                "UPDATE runs SET lease_until=?, heartbeat_at=? WHERE id=?",
                (lease_until, now, run_id))
            self.con.execute(
                "UPDATE resource_locks SET lease_until=?"
                " WHERE resource=? AND run_id=?",
                (lease_until, row["resource"], run_id))
            return {"ok": True, "status": "running", "lease_until": lease_until}

    def _release_lock(self, resource, run_id):
        self.con.execute(
            "UPDATE resource_locks SET run_id=NULL, lease_until=0"
            " WHERE resource=? AND run_id=?", (resource, run_id))

    def complete(self, run_id, epoch, ok=True, result=None, error=None,
                 now=None):
        """Record the final outcome; stale or late deliveries are rejected."""
        now = _now() if now is None else now
        with self._tx():
            row = self._run_row(run_id)
            if row is None:
                return {"accepted": False, "reason": "missing: no such run"}
            if row["status"] != "running" or row["epoch"] != epoch:
                self._event(run_id, "stale_result", now,
                            {"epoch": epoch, "current_epoch": row["epoch"],
                             "status": row["status"], "ok": ok,
                             "result": result, "error": error})
                return {"accepted": False, "status": row["status"],
                        "reason": "stale epoch %s (current %s, status %s)"
                                  % (epoch, row["epoch"], row["status"])}
            status = "succeeded" if ok else "failed"
            self.con.execute(
                "UPDATE runs SET status=?, finished_at=?, result=?, reason=?"
                " WHERE id=?",
                (status, now, _dump(result) if result is not None else None,
                 error, run_id))
            self._release_lock(row["resource"], run_id)
            self._event(run_id, "completed" if ok else "failed", now,
                        {"result": result, "error": error})
            return {"accepted": True, "status": status}

    def cancel(self, run_id, reason=None, now=None):
        """Cancel a pending or running run; terminal runs are left untouched.

        Cancelling a running run bumps its fencing epoch and releases the
        resource immediately, so the executor's next heartbeat fails and any
        late completion is rejected as stale.  Business data already written
        by the executor is deliberately left in place.
        """
        now = _now() if now is None else now
        with self._tx():
            row = self._run_row(run_id)
            if row is None:
                return {"ok": False, "reason": "missing: no such run"}
            if row["status"] in TERMINAL:
                self._event(run_id, "cancel_ignored", now,
                            {"status": row["status"], "reason": reason})
                return {"ok": False, "status": row["status"],
                        "reason": "already %s" % row["status"]}
            self.con.execute(
                "UPDATE runs SET status='cancelled', epoch=epoch+1,"
                " finished_at=?, reason=? WHERE id=?",
                (now, reason or "cancelled by user", run_id))
            self._release_lock(row["resource"], run_id)
            self._event(run_id, "cancelled", now,
                        {"reason": reason, "fenced_epoch": row["epoch"]})
            return {"ok": True, "status": "cancelled", "run_id": run_id}

    def reap(self, now=None, backoff_base=1.0, backoff_cap=60.0):
        """Requeue running runs whose lease expired (executor presumed lost).

        Each requeued run gets a fresh fencing epoch, a ``retry_at`` computed
        with exponential backoff, and an ``expired`` event (the overdue
        message) naming the lost executor.
        """
        now = _now() if now is None else now
        requeued = []
        with self._tx():
            rows = self.con.execute(
                "SELECT * FROM runs WHERE status='running' AND lease_until <= ?",
                (now,)).fetchall()
            for row in rows:
                delay = min(backoff_cap,
                            backoff_base * (2 ** max(0, row["attempts"] - 1)))
                retry_at = now + delay
                note = ("lease expired at %.3f; executor '%s' presumed lost"
                        % (row["lease_until"] or 0, row["executor"]))
                self.con.execute(
                    "UPDATE runs SET status='pending', epoch=epoch+1,"
                    " retry_at=?, reason=? WHERE id=?",
                    (retry_at, note, row["id"]))
                self._release_lock(row["resource"], row["id"])
                self._event(row["id"], "expired", now,
                            {"executor": row["executor"],
                             "lease_until": row["lease_until"],
                             "attempt": row["attempts"],
                             "retry_at": retry_at})
                requeued.append(row["id"])
        return requeued


# --------------------------------------------------------------------------- #
# Worker: claims runs and executes the collection pipeline under a lease.
# --------------------------------------------------------------------------- #
def default_job(mode, should_stop):
    """Run the real collection pipeline (the same entry as the manual CLI)."""
    from . import pipeline
    return pipeline.run(mode=mode)


def sleep_job(seconds):
    """Testing aid: a job that simulates work without touching the source."""
    def job(mode, should_stop):
        deadline = time.time() + seconds
        while time.time() < deadline:
            if should_stop.is_set():
                raise RuntimeError("cancelled while sleeping")
            time.sleep(min(0.05, max(0.0, deadline - time.time())))
        return {"engine": "sleep", "mode": mode, "slept": seconds}
    return job


class Worker:
    """Claims due runs and executes them, heartbeating in the background.

    If a heartbeat is rejected (cancelled, or the lease was re-claimed after
    expiring) ``should_stop`` is set so a cooperative job can abort early;
    either way the final ``complete`` is only accepted while the worker
    still holds the fencing epoch.
    """

    def __init__(self, store, executor_id=None, lease_s=30.0, heartbeat_s=None,
                 clock=_now, job_fn=None):
        self.store = store
        self.executor_id = executor_id or "worker-%d" % os.getpid()
        self.lease_s = float(lease_s)
        self.heartbeat_s = (float(heartbeat_s) if heartbeat_s is not None
                            else max(0.05, self.lease_s / 3.0))
        self.clock = clock
        self.job_fn = job_fn or default_job

    def work_once(self):
        ticket = self.store.claim_next(self.executor_id, lease_s=self.lease_s,
                                       now=self.clock())
        if not ticket:
            return None
        return self.execute(ticket)

    def execute(self, ticket):
        run_id, epoch = ticket["run_id"], ticket["epoch"]
        should_stop = threading.Event()
        stop_hb = threading.Event()
        lost = {"detail": None}

        def hb_loop():
            while not stop_hb.wait(self.heartbeat_s):
                ack = self.store.heartbeat(run_id, epoch, lease_s=self.lease_s,
                                           now=self.clock())
                if not ack.get("ok"):
                    lost["detail"] = ack.get("status") or ack.get("detail")
                    should_stop.set()
                    return

        hb = threading.Thread(target=hb_loop, name="bookdepth-hb-%s" % run_id,
                              daemon=True)
        hb.start()
        ok, result, error = False, None, None
        try:
            result = self.job_fn(ticket["mode"], should_stop)
            ok = True
        except Exception as exc:  # record any job failure as the run outcome
            error = "%s: %s" % (type(exc).__name__, exc)
        finally:
            stop_hb.set()
            hb.join(timeout=max(1.0, 2 * self.heartbeat_s))
        ack = self.store.complete(run_id, epoch, ok=ok, result=result,
                                  error=error, now=self.clock())
        return {"run_id": run_id, "epoch": epoch, "job_ok": ok, "error": error,
                "recorded": bool(ack.get("accepted")),
                "record_status": ack.get("status"),
                "record_reason": ack.get("reason"),
                "lost_lease": lost["detail"]}


def serve(store, worker, interval=1.0, once=False, clock=_now,
          sleeper=time.sleep, max_cycles=None):
    """Controller loop: raise due scheduled runs, requeue expired leases, work."""
    outcome = None
    cycles = 0
    while True:
        store.tick(now=clock())
        store.reap(now=clock())
        outcome = worker.work_once()
        cycles += 1
        if once or (max_cycles is not None and cycles >= max_cycles):
            return outcome
        if outcome is None:
            sleeper(interval)


# --------------------------------------------------------------------------- #
# Command-line entry.
# --------------------------------------------------------------------------- #
def _build_parser():
    import argparse
    p = argparse.ArgumentParser(
        prog="bookdepth.manage",
        description="Single-machine run management for bookdepth collection.")
    p.add_argument("--db", default=None,
                   help="management DB path (default: the bookdepth database)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("submit", help="submit a manual catch-up run")
    sp.add_argument("--mode", choices=list(MODES), default="full")
    sp.add_argument("--resource", default=DEFAULT_RESOURCE)
    sp.add_argument("--delay", type=float, default=0.0,
                    help="seconds before the run becomes due")

    sp = sub.add_parser("schedule", help="manage recurring schedules")
    ssub = sp.add_subparsers(dest="action", required=True)
    a = ssub.add_parser("add")
    a.add_argument("--name", required=True)
    a.add_argument("--mode", choices=list(MODES), default="incremental")
    a.add_argument("--interval", type=float, required=True)
    a.add_argument("--resource", default=DEFAULT_RESOURCE)
    a.add_argument("--start-in", type=float, default=None,
                   help="seconds until the first run (default: one interval)")
    ssub.add_parser("list")
    r = ssub.add_parser("remove")
    r.add_argument("--name", required=True)

    sub.add_parser("tick", help="create runs for schedules that fell due")
    sub.add_parser("reap", help="requeue runs whose lease expired")

    sp = sub.add_parser("work", help="claim and execute due runs")
    sp.add_argument("--once", action="store_true")
    sp.add_argument("--interval", type=float, default=1.0)
    sp.add_argument("--lease", type=float, default=30.0)
    sp.add_argument("--executor", default=None)
    sp.add_argument("--sleep", type=float, default=None, metavar="SECONDS",
                    help="testing aid: run a sleep job instead of the pipeline")

    sp = sub.add_parser("cancel", help="cancel a pending or running run")
    sp.add_argument("--run", type=int, required=True)
    sp.add_argument("--reason", default=None)

    sp = sub.add_parser("status", help="show one run (or all runs)")
    sp.add_argument("--run", type=int, default=None)

    sp = sub.add_parser("list", help="list recent runs")
    sp.add_argument("--limit", type=int, default=50)
    return p


def _dispatch(args, store):
    if args.cmd == "submit":
        run_id = store.submit(resource=args.resource, mode=args.mode,
                              trigger="manual",
                              due_at=_now() + max(0.0, args.delay))
        return {"run_id": run_id, "run": store.get_run(run_id)}
    if args.cmd == "schedule":
        if args.action == "add":
            start_at = None if args.start_in is None else _now() + args.start_in
            store.add_schedule(args.name, mode=args.mode,
                               interval_s=args.interval,
                               resource=args.resource, start_at=start_at)
        elif args.action == "remove":
            return {"removed": store.remove_schedule(args.name)}
        return {"schedules": store.schedules()}
    if args.cmd == "tick":
        return {"created": store.tick()}
    if args.cmd == "reap":
        return {"requeued": store.reap()}
    if args.cmd == "work":
        job_fn = sleep_job(args.sleep) if args.sleep is not None else None
        worker = Worker(store, executor_id=args.executor, lease_s=args.lease,
                        job_fn=job_fn)
        outcome = serve(store, worker, interval=args.interval, once=args.once)
        return {"outcome": outcome}
    if args.cmd == "cancel":
        return store.cancel(args.run, reason=args.reason)
    if args.cmd == "status":
        if args.run is not None:
            run = store.get_run(args.run)
            return run if run is not None else {"error": "no such run",
                                                "run_id": args.run}
        return {"runs": store.list_runs()}
    if args.cmd == "list":
        return {"runs": store.list_runs(limit=args.limit)}
    raise SystemExit("unknown command")


def main(argv=None):
    args = _build_parser().parse_args(argv)
    store = ManageStore(db_path=args.db)
    try:
        out = _dispatch(args, store)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
