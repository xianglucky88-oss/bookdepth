"""Tests for order-book persistence: slots, audit, journal and migration."""
import sqlite3

import pytest

from bookdepth.store import Store


@pytest.fixture()
def store():
    con = sqlite3.connect(":memory:")
    st = Store(con)
    try:
        yield st
    finally:
        con.close()


def _quote(symbol="AAA", level=1, side="bid", price=100.0, size=5.0,
           source="explicit"):
    return {"symbol": symbol, "level": level, "side": side,
            "price": price, "size": size, "ts": "t1", "source": source,
            "issues": [], "evidence": ["field:side=bid"], "raw": {"x": 1}}


def test_book_slot_is_first_writer_wins(store):
    q1 = _quote(price=100.0)
    q2 = _quote(price=99.5, source="late")
    assert store.upsert_book_slot(q1) is True
    assert store.upsert_book_slot(q2) is False       # never overwritten
    owner = store.get_book_slot(q1)
    assert owner["price"] == 100.0
    assert owner["raw"] == {"x": 1}
    assert store.book_slot_count() == 1


def test_audit_events_are_idempotent(store):
    payload = {"reason": "blocked", "incoming": 99.5}
    assert store.audit_book_event("slot_overwrite_blocked", payload,
                                  symbol="AAA", level=1, side="bid") is True
    # Same kind+payload produces the same hash and inserts nothing new.
    assert store.audit_book_event("slot_overwrite_blocked", dict(payload),
                                  symbol="AAA", level=1, side="bid") is False
    assert store.audit_event_count() == 1
    event = store.all_audit_events()[0]
    assert event["kind"] == "slot_overwrite_blocked"
    assert event["level"] == 1


def test_journal_distinguishes_journaled_from_applied(store):
    r1, r2 = {"id": "a", "v": 1}, {"id": "b", "v": 2}
    store.journal_page("run-1", 1, [r1, r2])
    assert store.journal_seen("run-1") == {Store.hash(r1), Store.hash(r2)}
    assert store.journal_applied("run-1") == set()
    store.journal_mark_applied("run-1", Store.hash(r1))
    assert store.journal_applied("run-1") == {Store.hash(r1)}
    # Re-journaling the same page neither duplicates rows nor resets flags.
    store.journal_page("run-1", 1, [r1, r2])
    assert store.journal_applied("run-1") == {Store.hash(r1)}
    assert store.journal_count("run-1") == 2


def test_journal_is_scoped_per_run(store):
    store.journal_page("run-a", 1, [{"id": "a"}])
    store.journal_page("run-b", 1, [{"id": "a"}])
    assert store.journal_count("run-a") == 1
    assert store.journal_count("run-b") == 1
    assert store.journal_seen("run-other") == set()


def test_book_watermark_is_independent_of_legacy_watermark(store):
    store.set_watermark("2030-01-01")
    store.set_book_watermark("2030-02-01")
    assert store.watermark() == "2030-01-01"
    assert store.book_watermark() == "2030-02-01"


def test_legacy_upsert_and_records_table_remain_unchanged(store):
    record = {"id": "sample", "ts": "t1", "v": 1}
    assert store.upsert(record) is True
    assert store.upsert(record) is False
    assert store.count() == 1
    assert store.get(Store.hash(record)) == record


def test_migrates_legacy_journal_schema_without_applied_column():
    # Simulate a database created before the applied flag existed.
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, "
                "id TEXT, ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE run_journal(run_id TEXT NOT NULL, seq INTEGER "
                "NOT NULL, content_hash TEXT NOT NULL, payload TEXT NOT NULL, "
                "PRIMARY KEY(run_id, seq))")
    con.execute("INSERT INTO run_journal VALUES('r', 1, 'h1', '{}')")
    con.commit()
    store = Store(con)                    # triggers _init + migration
    try:
        cols = {row[1] for row in
                con.execute("PRAGMA table_info(run_journal)").fetchall()}
        assert "applied" in cols
        # Existing rows default to not applied so resume reprocesses them.
        assert store.journal_applied("r") == set()
        store.journal_mark_applied("r", "h1")
        assert store.journal_applied("r") == {"h1"}
    finally:
        con.close()
