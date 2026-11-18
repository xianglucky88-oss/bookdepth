"""Tests for the durable order-book pipeline.

Covers throttled retries, duplicate pages, historical replay, interrupt/
resume and full-vs-incremental spread stability, while asserting that the
existing pacing, backfill watermark and content-hash dedup behavior is
preserved.
"""
import json
import sqlite3

import pytest

from bookdepth import framework, pipeline
from bookdepth.framework import (
    Http, Sleeper, Transport, Interrupted, fetch_book_pages,
)
from bookdepth.store import Store
from mockapp.bookdata import base_rows


def _mem_store():
    """An isolated in-memory store, never touching an on-disk database."""
    con = sqlite3.connect(":memory:")
    return con, Store(con)


@pytest.fixture()
def store():
    # In-memory database shared through one connection, fully isolated from
    # any on-disk bookdepth.db used by manual CLI runs.
    import sqlite3
    con = sqlite3.connect(":memory:")
    st = Store(con)
    try:
        yield st
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# Transport: rate-limit retries and duplicate pages.
# --------------------------------------------------------------------------- #
def test_rate_limit_retries_succeed_without_data_loss(store):
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    records, attempts, pages = fetch_book_pages(http)
    # The fixture 429s on the first two calls of a fresh app instance, then
    # serves four pages: 6 HTTP calls for 12 rows, no rows lost.
    assert attempts == 6 and pages == 4
    assert len(records) == 12
    # Politeness delay per call plus two exponential backoffs.
    assert sleeper.calls >= attempts + 2
    assert sleeper.slept > 0


def test_duplicate_page_content_is_deduped_by_content_hash(store):
    first = pipeline.run_book(con=store.con)
    unique_in_batch = first["fetched"] - first["duplicates"]
    before_slots = store.book_slot_count()
    # A second full run replays identical pages: nothing new may be written.
    second = pipeline.run_book(con=store.con)
    assert store.book_slot_count() == before_slots
    assert second["processed"] == 0
    assert second["replayed"] == unique_in_batch
    # Audit trail is idempotent too.
    assert second["audit_events"] == first["audit_events"]


def test_dedup_keeps_original_content_hash_semantics(store):
    # framework.dedup on canonical payloads still uses the same content-hash
    # rule as the classic pipeline; only exact content counts as a duplicate.
    from bookdepth.framework import dedup
    sample = {"id": "x", "price": 1}
    out, n = dedup([dict(sample), dict(sample), {"other": 1}])
    assert n == 1 and len(out) == 2
    # A differing field means a different hash, never silently merged.
    out2, n2 = dedup([{"id": "x", "price": 1}, {"id": "x", "price": 2}])
    assert n2 == 0 and len(out2) == 2


# --------------------------------------------------------------------------- #
# Full run: direction correctness, conflicts, crossed book and raw fidelity.
# --------------------------------------------------------------------------- #
def test_full_run_resolves_directions_and_audits_every_issue(store):
    result = pipeline.run_book(con=store.con)
    assert result["completed"] is True
    slots = {(s["symbol"], s["level"], s["side"]): s
             for s in store.all_book_slots()}

    # Ask-before-bid still yields one canonical bid and ask slot per level.
    aaa1_bid = slots[("AAA", 1, "bid")]
    aaa1_ask = slots[("AAA", 1, "ask")]
    assert aaa1_bid["price"] == 100.0 and aaa1_ask["price"] == 101.0

    # The later 99.5 bid must not overwrite the earlier 100.0 bid.
    assert aaa1_bid["raw"]["price"] == 100.0

    blocked = [e for e in store.all_audit_events()
               if e["kind"] == framework._direction.ISSUE_SLOT_BLOCKED]
    assert len(blocked) == 1
    assert blocked[0]["payload"]["incoming"]["price"] == 99.5

    # Explicit/embedded contradiction and missing direction are unresolved.
    assert result["unresolved"] == 2
    kinds = {e["kind"] for e in store.all_audit_events()}
    assert "missing_side" in kinds

    # Crossed book flagged, sign never repaired.
    assert result["crossed"] == 1
    ddd_ask = slots[("DDD", 1, "ask")]
    assert ddd_ask["price"] == 59.0

    # Unparseable price retained as raw text, stored price stays NULL.
    eee = slots[("EEE", 1, "bid")]
    assert eee["price"] is None
    assert eee["raw"]["price"] == "abc"


def test_spread_is_stable_between_full_runs(store):
    first = pipeline.run_book(con=store.con)
    second = pipeline.run_book(con=store.con)
    assert first["spreads"] == second["spreads"]
    assert first["valid_spreads"] == second["valid_spreads"]
    # The fixture contains one genuinely crossed level.
    assert first["acceptable_spread"] is False
    assert -1.0 in first["spreads"]


# --------------------------------------------------------------------------- #
# Historical replay.
# --------------------------------------------------------------------------- #
def test_historical_replay_is_idempotent_and_order_independent(store):
    rows = base_rows()
    r1 = pipeline.replay_book(rows, con=store.con, run_id="rp")
    # Replay the same rows twice more, shuffled — outcome must not change.
    r2 = pipeline.replay_book(list(reversed(rows)), con=store.con, run_id="rp")
    r3 = pipeline.replay_book(rows, con=store.con, run_id="rp")
    assert r2["processed"] == 0 and r2["replayed"] == r1["processed"]
    assert r3["audit_events"] == r1["audit_events"]
    assert store.book_slot_count() == r1["slots_total"]
    # First-writer-wins preserved even with reversed arrival.
    bid = next(s for s in store.all_book_slots()
               if s["symbol"] == "AAA" and s["level"] == 1 and s["side"] == "bid")
    assert bid["price"] == 100.0


def test_replay_and_live_run_share_the_same_direction_rules(store):
    live = pipeline.run_book(con=store.con)
    replay = pipeline.replay_book(base_rows(), con=store.con, run_id="rp2")
    live_slots = sorted((s["symbol"], s["level"], s["side"], s["price"])
                        for s in store.all_book_slots())
    # Replay on a clean store produces identical canonical slots.
    fresh_con, fresh = _mem_store()
    try:
        pipeline.replay_book(base_rows(), con=fresh_con, run_id="solo")
        replay_slots = sorted((s["symbol"], s["level"], s["side"], s["price"])
                              for s in fresh.all_book_slots())
    finally:
        fresh_con.close()
    assert replay_slots == [row for row in live_slots]
    assert replay["blocked"] == live["blocked"] == 1
    assert replay["crossed"] == live["crossed"] == 1


# --------------------------------------------------------------------------- #
# Interrupt / resume.
# --------------------------------------------------------------------------- #
def test_interrupted_run_resumes_without_applying_twice(store):
    run_id = "resume-run"
    with pytest.raises(Interrupted) as exc:
        pipeline.run_book(con=store.con, run_id=run_id, fail_after=5)
    assert exc.value.processed == 5
    checkpoint = store.meta_get(framework._checkpoint_key(run_id))
    assert checkpoint is not None
    slots_after_interrupt = store.book_slot_count()

    # Resume from the checkpointed (interrupted) page.  Rows already in the
    # journal are skipped; the remainder of the page is finished normally.
    result = pipeline.run_book(con=store.con, run_id=run_id, resume=True)
    assert result["completed"] is True
    assert result["resumed_from_page"] == int(checkpoint)
    assert result["replayed"] >= 1
    assert result["processed"] + exc.value.processed == 11  # unique rows total
    assert store.book_slot_count() > slots_after_interrupt

    # Final book equals an uninterrupted full run on a clean database,
    # including durable issue counters and the reported spread vector.
    clean, clean_st = _mem_store()
    try:
        clean_summary = pipeline.run_book(con=clean, run_id="clean")
        resumed_slots = sorted((s["symbol"], s["level"], s["side"], s["price"])
                               for s in store.all_book_slots())
        clean_slots = sorted((s["symbol"], s["level"], s["side"], s["price"])
                             for s in clean_st.all_book_slots())
    finally:
        clean.close()
    assert resumed_slots == clean_slots
    for key in ("blocked", "unresolved", "crossed", "audit_events",
                "slots_total", "spreads", "valid_spreads",
                "acceptable_spread", "levels_with_spread"):
        assert result[key] == clean_summary[key], key
    # Every unique record carries the applied marker (the duplicated page
    # row shares a hash with its first occurrence: 11 hashes for 12 rows).
    assert len(store.journal_applied(run_id)) == 11
    # All 11 unique hashes are present in the journal.
    assert len(store.journal_seen(run_id)) == 11
    # Checkpoint is cleared once the run completes.
    assert store.meta_get(framework._checkpoint_key(run_id)) is None


def test_resume_at_every_possible_cut_matches_a_clean_run(store):
    # Exhaustively interrupt after each unique row; every cut must resume to
    # exactly the same durable book as an uninterrupted run.
    for cut in range(1, 11):
        con, st = _mem_store()
        try:
            with pytest.raises(Interrupted):
                pipeline.run_book(con=con, run_id="cut-%d" % cut,
                                  fail_after=cut)
            pipeline.run_book(con=con, run_id="cut-%d" % cut, resume=True)
            slots = sorted((s["symbol"], s["level"], s["side"], s["price"])
                           for s in st.all_book_slots())
            audits = st.audit_event_count()
        finally:
            con.close()

        clean_con, clean_st = _mem_store()
        try:
            pipeline.run_book(con=clean_con, run_id="ref-%d" % cut)
            ref = sorted((s["symbol"], s["level"], s["side"], s["price"])
                         for s in clean_st.all_book_slots())
            ref_audits = clean_st.audit_event_count()
        finally:
            clean_con.close()
        assert slots == ref
        assert audits == ref_audits


def test_resume_without_checkpoint_is_an_explicit_error(store):
    with pytest.raises(RuntimeError):
        pipeline.run_book(con=store.con, run_id="missing", resume=True)


# --------------------------------------------------------------------------- #
# Incremental collection and unchanged pacing/dedup/watermark behavior.
# --------------------------------------------------------------------------- #
def test_incremental_picks_up_only_newer_rows_with_stable_spreads(store):
    full = pipeline.run_book(con=store.con)
    assert full["watermark"] == "2030-02-04T00:00:00Z"

    # Nothing new yet: incremental run writes nothing and keeps spreads.
    idle = pipeline.run_book(mode="incremental", con=store.con)
    assert idle["processed"] == 0
    assert idle["spreads"] == full["spreads"]

    # The fixture gains rows with strictly newer timestamps.
    Transport().request("GET", "/book/refresh")
    inc = pipeline.run_book(mode="incremental", con=store.con)
    assert inc["processed"] == 2
    assert inc["watermark"] == "2030-02-09T00:00:00Z"
    # Old spreads remain present; only the new level is added.
    for spread in full["valid_spreads"]:
        assert spread in inc["spreads"]
    assert any(s["symbol"] == "AAA" and s["level"] == 4
               for s in store.all_book_slots())


def test_pacing_sleep_matches_the_existing_http_contract():
    # Direct check against Http/Sleeper semantics: politeness every call,
    # exponential backoff only on retryable statuses, same contract as the
    # classic crawler — request rhythm is unchanged.  A custom transport
    # scripted to fail twice removes dependence on shared fixture state.
    class ScriptedTransport:
        def __init__(self):
            self.n = 0

        def request(self, method, path, params=None, json_body=None,
                    headers=None):
            self.n += 1
            if self.n <= 2:
                return 429, {}, ""
            return 200, {}, json.dumps({"items": [], "has_more": False})

    sleeper = Sleeper()
    http = Http(ScriptedTransport(), sleeper, max_retries=4,
                base_backoff=0.5, politeness=0.25)
    _status, _hdr, _body, attempts = http.get("/book")
    assert attempts == 2                                 # two retries
    expected_politeness = 3 * 0.25
    expected_backoff = 0.5 * (2 ** 0) + 0.5 * (2 ** 1)
    assert sleeper.slept == pytest.approx(expected_politeness + expected_backoff)
    assert sleeper.calls == 5


def test_classic_pipeline_remains_unchanged(store):
    # The legacy crawler path must keep its original output shape and
    # watermark behavior alongside the new book pipeline.
    result = pipeline.run(con=store.con)
    assert result["engine"] == "crawl"
    assert {"fetched", "duplicates", "ingested", "count",
            "http_attempts", "slept", "watermark"} <= set(result)
    assert store.watermark() == result["watermark"]
