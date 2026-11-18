"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import hashlib
import importlib
import json

from lxml import html as _lh

from .config import SPEC
from .store import Store
from . import direction as _direction

# Internal tag carrying the source page number.  Stripped before any record
# is hashed, journaled or persisted so it never changes content identity.
PAGE_TAG = "_book_page"

# Meta key holding the page at which an interrupted run should resume.
def _checkpoint_key(run_id):
    return "book_checkpoint:%s" % run_id


class Interrupted(RuntimeError):
    """Raised when a run stops early; the journal lets it resume later."""

    def __init__(self, run_id, processed, page, total_seen):
        super().__init__("book run interrupted run_id=%s processed=%s page=%s"
                         % (run_id, processed, page))
        self.run_id = run_id
        self.processed = processed
        self.page = page
        self.total_seen = total_seen


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import os
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


def fetch_all(http: Http):
    """Collect records from the local source."""
    records, attempts, page = [], 0, 1
    while True:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        attempts += att + 1
        if status != 200:
            raise RuntimeError("fetch failed status=%s page=%s" % (status, page))
        page_recs = parse(body, SPEC.fmt)
        records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            break
        page += 1
        if page > 200:
            break
    return records, attempts


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(d, sort_keys=True, ensure_ascii=False)


def content_hash(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def run(mode="full", con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    try:
        records, attempts = fetch_all(http)
        norm = [normalize(dict(r)) for r in records]
        norm, dup_in_batch = dedup(norm)
        norm = _apply_kernel(norm)

        wm = store.watermark()
        if mode == "incremental" and wm:
            norm = [r for r in norm if str(r.get("ts", "")) > wm]

        ingested = sum(1 for r in norm if store.upsert(r))
        total = store.count()
        new_wm = max([str(r.get("ts", "")) for r in norm] + [wm or ""])
        if new_wm:
            store.set_watermark(new_wm)
        return {"engine": "crawl", "mode": mode, "fetched": len(records),
                "duplicates": dup_in_batch, "ingested": ingested, "count": total,
                "http_attempts": attempts, "slept": round(sleeper.slept, 4),
                "watermark": store.watermark()}
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    r = Rpa()
    try:
        logged = r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        norm, dup = dedup(norm)
        norm = _apply_kernel(norm)
        ingested = sum(1 for x in norm if store.upsert(x))
        total = store.count()
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        wm = max([str(x.get("ts", "")) for x in norm] + [""])
        if wm:
            store.set_watermark(wm)
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": total,
                "submit_created": created, "submit_dedup": dedup_submit, "log": r.log}
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Order-book collection pipeline (reliable bid/ask direction).
# --------------------------------------------------------------------------- #
def fetch_book_pages(http, start_page=1, page_size=None):
    """Fetch every /book page with the existing retry/pacing machinery.

    Returns the tagged records (each carrying an internal ``_book_page``),
    total HTTP attempts and the number of pages read.  Reused unchanged for
    full runs, incremental runs, replays and interrupted resumes.
    """
    page_size = page_size or SPEC.book_page_size
    records, attempts, page, pages_read = [], 0, start_page, 0
    while True:
        status, _hdr, body, att = http.get("/book", params={"page": page})
        attempts += att + 1
        if status != 200:
            raise RuntimeError("book fetch failed status=%s page=%s" % (status, page))
        data = json.loads(body)
        page_recs = data.get("items", [])
        pages_read += 1
        for rec in page_recs:
            tagged = dict(rec)
            tagged[PAGE_TAG] = page
            records.append(tagged)
        if not data.get("has_more"):
            break
        page += 1
        if page > 200:
            break
    return records, attempts, pages_read


def _strip_tag(record):
    clean = dict(record)
    clean.pop(PAGE_TAG, None)
    return clean


def view_from_slots(store):
    """Rebuild the canonical book view from durable slots.

    This keeps spreads identical across full runs, incremental runs and
    replays: the reported book always reflects what is stored, not just the
    records observed in the current invocation.
    """
    state = _direction.new_book_state()
    for slot in store.all_book_slots():
        _direction.merge_quote(state, {
            "symbol": slot["symbol"], "level": slot["level"],
            "side": slot["side"], "price": slot["price"],
            "size": slot["size"], "ts": slot["ts"],
            "source": slot["source"], "issues": slot["issues"],
            "evidence": slot["evidence"], "raw": slot["raw"],
        })
    return _direction.finalize_book(state)


def _audit_quote_issues(store, quote):
    """Persist one audit row per issue carried by a single quote."""
    kind = "quote_issues"
    payload = {"issues": list(quote.get("issues", [])),
               "evidence": list(quote.get("evidence", [])),
               "source": quote.get("source"),
               "raw": quote.get("raw", {})}
    store.audit_book_event(kind, payload, symbol=quote.get("symbol"),
                           level=quote.get("level"), side=quote.get("side"),
                           ts=quote.get("ts"))


def _persist_book(store, pages, run_id, fail_after=None):
    """Apply page-grouped records to the durable book.

    Each page is journaled before its records are applied, so an interrupt
    at any point can resume from the interrupted page: rows already in the
    journal are skipped, the rest of the page is finished. Slot ownership is
    decided against the *durable* slots, so conflict detection is identical
    whether a run completes in one pass or is interrupted and resumed.
    """
    applied_hashes = store.journal_applied(run_id)
    seen_hashes = set()      # rows handled in this invocation
    processed = resumed = quotes_written = 0
    blocked = unresolved = crossed = 0

    for page, records in pages:
        # Journal the whole page first; the journal is the durable resume
        # source.  Rows marked applied from a previous (interrupted) attempt
        # are skipped; rows only journaled are retried.
        store.journal_page(run_id, page, records)
        for record in records:
            h = Store.hash(record)
            if h in applied_hashes:
                # Already fully processed before an interrupt: never twice.
                resumed += 1
                continue
            if h in seen_hashes:
                # Duplicate content within this invocation (re-sent pages).
                resumed += 1
                continue

            for quote in _direction.extract_quotes(record):
                side = quote.get("side")
                if side not in (_direction.BID, _direction.ASK):
                    unresolved += 1
                    store.audit_book_event(
                        _direction.ISSUE_MISSING_SIDE,
                        {"quote": quote, "evidence": quote.get("evidence", []),
                         "raw": quote.get("raw", {})},
                        symbol=quote.get("symbol"), level=quote.get("level"),
                        side=_direction.UNKNOWN, ts=quote.get("ts"))
                    _audit_quote_issues(store, quote)
                    continue

                owner = store.get_book_slot(quote)
                if owner is not None:
                    same = (owner.get("price") == quote.get("price")
                            and owner.get("size") == quote.get("size"))
                    if same:
                        # Duplicate content for an already-owned slot; the
                        # quote issues are still audited, no slot changes.
                        _audit_quote_issues(store, quote)
                        continue
                    # Contradictory later quote: the durable first writer is
                    # kept and the rejection is fully traceable.
                    blocked += 1
                    store.audit_book_event(
                        _direction.ISSUE_SLOT_BLOCKED,
                        {"existing": owner, "incoming": quote,
                         "evidence": ["existing_source:%s" % owner.get("source"),
                                      "incoming_source:%s" % quote.get("source")]
                                      + list(quote.get("evidence", []))},
                        symbol=quote.get("symbol"), level=quote.get("level"),
                        side=side, ts=quote.get("ts"))
                    _audit_quote_issues(store, quote)
                    continue

                if store.upsert_book_slot(quote):
                    quotes_written += 1
                _audit_quote_issues(store, quote)

            # All quotes of this record have been handled durably.
            store.journal_mark_applied(run_id, h)
            applied_hashes.add(h)
            seen_hashes.add(h)
            processed += 1

            if fail_after is not None and processed >= fail_after:
                # The current page is fully journaled, so resume re-fetches
                # it and the journal skips the already-applied rows.
                store.meta_set(_checkpoint_key(run_id), page)
                raise Interrupted(run_id, processed, page,
                                  total_seen=processed + resumed)

    view = view_from_slots(store)
    for level in view["levels"]:
        if level["crossed"]:
            crossed += 1
            store.audit_book_event(
                _direction.ISSUE_CROSSED,
                {"symbol": level["symbol"], "level": level["level"],
                 "bid_price": level["bid_price"],
                 "ask_price": level["ask_price"],
                 "spread": level["spread"]},
                symbol=level["symbol"], level=level["level"])
    return {"view": view, "processed": processed,
            "resumed": resumed, "quotes_written": quotes_written,
            "blocked": blocked, "unresolved": unresolved,
            "crossed": crossed}


def run_book(mode="full", con=None, run_id="book", fail_after=None,
             resume=False):
    """Collect /book pages and normalize bid/ask direction durably.

    ``mode`` mirrors the classic pipeline: ``full`` ingests everything,
    ``incremental`` only rows newer than the stored book watermark.
    ``fail_after`` simulates an interrupt; a follow-up call with
    ``resume=True`` continues from the checkpointed page and the journal
    guarantees no content is applied twice.
    """
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    checkpoint = store.meta_get(_checkpoint_key(run_id))
    try:
        start_page = 1
        if resume:
            if checkpoint is None:
                raise RuntimeError("no checkpoint found for run_id=%s" % run_id)
            start_page = int(checkpoint)
        records, attempts, pages_read = fetch_book_pages(http, start_page)

        # Same content-hash duplicate semantics as the classic pipeline.
        norm = [normalize(_strip_tag(dict(r))) for r in records]
        norm, dup_in_batch = dedup(norm)
        # Re-attach page tags to the surviving canonical records.
        tag_by_hash = {Store.hash(normalize(_strip_tag(dict(r)))):
                       r.get(PAGE_TAG) for r in records}
        for r in norm:
            r[PAGE_TAG] = tag_by_hash.get(Store.hash(r))

        wm = store.book_watermark()
        if mode == "incremental" and wm:
            norm = [r for r in norm if str(r.get("ts", "")) > wm]

        # Group surviving records by their source page for page-level
        # journaling; ordering within a page is preserved.
        grouped = {}
        for r in norm:
            grouped.setdefault(r.get(PAGE_TAG), []).append(_strip_tag(r))
        pages = sorted(grouped.items(),
                       key=lambda item: (item[0] is None, item[0] or 0))

        result = _persist_book(store, pages, run_id=run_id,
                               fail_after=fail_after)
        # Completed run: checkpoint is no longer needed.
        store.meta_delete(_checkpoint_key(run_id))
        new_wm = max([str(r.get("ts", "")) for r in norm] + [wm or ""])
        if new_wm:
            store.set_book_watermark(new_wm)

        view = view_from_slots(store)
        levels = [lvl for lvl in view["levels"]
                  if lvl["spread_available"]]
        spreads = sorted(lvl["spread"] for lvl in levels)
        valid_spreads = sorted(lvl["spread"] for lvl in levels
                               if lvl["valid_order"])
        # Issue counters reflect the durable audit trail, so an interrupted
        # run and its resume together report the same totals as one clean run.
        audit_counts = store.audit_counts_by_kind()
        blocked_total = audit_counts.get(_direction.ISSUE_SLOT_BLOCKED, 0)
        unresolved_total = audit_counts.get(_direction.ISSUE_MISSING_SIDE, 0)
        crossed_total = sum(1 for lvl in levels if lvl["crossed"])
        # A run is acceptable when every level that has both sides carries a
        # non-negative spread; crossed levels are always reported, never
        # repaired by flipping a sign.
        acceptable = bool(levels) and len(valid_spreads) == len(levels)
        return {
            "engine": "book", "mode": mode, "run_id": run_id,
            "fetched": len(records), "duplicates": dup_in_batch,
            "pages_read": pages_read, "resumed_from_page": start_page,
            "processed": result["processed"], "replayed": result["resumed"],
            "quotes_written": result["quotes_written"],
            "slots_total": store.book_slot_count(),
            "blocked": blocked_total, "unresolved": unresolved_total,
            "crossed": crossed_total,
            "audit_events": store.audit_event_count(),
            "http_attempts": attempts, "slept": round(sleeper.slept, 4),
            "spreads": spreads, "valid_spreads": valid_spreads,
            "levels_with_spread": len(levels),
            "acceptable_spread": acceptable,
            "watermark": store.book_watermark(),
            "completed": True,
        }
    finally:
        if own:
            con.close()


def replay_book(records, con=None, run_id="book-replay"):
    """Replay historical records through the same durable rules.

    No fetching happens; the journal and audit idempotency make the outcome
    identical whether the rows arrive once or many times.
    """
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    try:
        norm = [normalize(dict(r)) for r in records]
        norm, dup_in_batch = dedup(norm)
        result = _persist_book(store, [(None, norm)], run_id=run_id)
        view = view_from_slots(store)
        audit_counts = store.audit_counts_by_kind()
        return {
            "engine": "book", "mode": "replay", "run_id": run_id,
            "fetched": len(records), "duplicates": dup_in_batch,
            "processed": result["processed"], "replayed": result["resumed"],
            "quotes_written": result["quotes_written"],
            "slots_total": store.book_slot_count(),
            "blocked": audit_counts.get(_direction.ISSUE_SLOT_BLOCKED, 0),
            "unresolved": audit_counts.get(_direction.ISSUE_MISSING_SIDE, 0),
            "crossed": sum(1 for lvl in view["levels"] if lvl["crossed"]),
            "audit_events": store.audit_event_count(),
            "spreads": sorted(lvl["spread"] for lvl in view["levels"]
                              if lvl["spread_available"]),
            "completed": True,
        }
    finally:
        if own:
            con.close()
