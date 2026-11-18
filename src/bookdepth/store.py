
"""Real sqlite3 store: content-hash primary key, INSERT-OR-IGNORE dedup, watermark."""
import json, sqlite3, hashlib
from .config import DB_PATH

class Store:
    def __init__(self, con=None):
        self.con = con
        if con is not None:
            self._init()

    def connect(self):
        con = sqlite3.connect(DB_PATH)
        self.__init__(con)
        return con

    def _init(self):
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS records("
            "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        # Canonical order-book slots.  The primary key is derived from the
        # slot identity (symbol, level, side), so a later, contradictory
        # quote can never replace an earlier one: INSERT OR IGNORE keeps the
        # first writer and the rejected quote is recorded in book_audit.
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS book_slots("
            "slot_hash TEXT PRIMARY KEY, symbol TEXT, level TEXT, side TEXT, "
            "price REAL, size REAL, source TEXT, ts TEXT, "
            "issues TEXT, evidence TEXT, raw TEXT NOT NULL)")
        # Auditable trail for conflicts, nulls, missing/unknown directions
        # and every other issue.  Keyed by a content hash of the event so
        # replaying history never produces duplicate audit rows.
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS book_audit("
            "audit_hash TEXT PRIMARY KEY, kind TEXT, symbol TEXT, "
            "level TEXT, side TEXT, ts TEXT, payload TEXT NOT NULL)")
        # Per-run processing journal powering interrupt/resume.  ``applied``
        # flips to 1 only after a record's quotes have been processed, so a
        # row that was journaled just before a crash is retried on resume
        # instead of being mistaken for already-applied work.
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS run_journal("
            "run_id TEXT NOT NULL, seq INTEGER NOT NULL, "
            "content_hash TEXT NOT NULL, payload TEXT NOT NULL, "
            "applied INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY(run_id, seq))")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_journal_run "
            "ON run_journal(run_id, content_hash)")
        self._migrate()
        self.con.commit()

    def _migrate(self):
        """Upgrade databases created by earlier versions in place."""
        cols = {row[1] for row in
                self.con.execute("PRAGMA table_info(run_journal)").fetchall()}
        if "applied" not in cols:
            self.con.execute(
                "ALTER TABLE run_journal ADD COLUMN applied INTEGER NOT NULL "
                "DEFAULT 0")

    @staticmethod
    def hash(record):
        blob = json.dumps(record, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode()).hexdigest()[:24]

    def upsert(self, record):
        cur = self.con.execute(
            "INSERT OR IGNORE INTO records(content_hash,id,ts,payload) VALUES(?,?,?,?)",
            (self.hash(record), str(record.get("id", "")),
             str(record.get("ts", "")), json.dumps(record, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def count(self):
        return self.con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def all_records(self):
        return [json.loads(p) for (_h, p) in
                self.con.execute("SELECT content_hash,payload FROM records")]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM records WHERE content_hash=?", (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    # ------------------------------------------------------------------ #
    # Generic meta key/value (watermark helpers below stay as before).
    # ------------------------------------------------------------------ #
    def meta_get(self, key):
        row = self.con.execute(
            "SELECT v FROM meta WHERE k=?", (str(key),)).fetchone()
        return row[0] if row else None

    def meta_set(self, key, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (str(key), str(value)))
        self.con.commit()

    def meta_delete(self, key):
        self.con.execute("DELETE FROM meta WHERE k=?", (str(key),))
        self.con.commit()

    def watermark(self):
        return self.meta_get("watermark")

    def set_watermark(self, value):
        self.meta_set("watermark", value)

    def book_watermark(self):
        return self.meta_get("book_watermark")

    def set_book_watermark(self, value):
        self.meta_set("book_watermark", value)

    # ------------------------------------------------------------------ #
    # Order-book slots and audit trail.
    # ------------------------------------------------------------------ #
    @staticmethod
    def slot_hash(quote):
        identity = {"symbol": quote.get("symbol"),
                    "level": quote.get("level"),
                    "side": quote.get("side")}
        return Store.hash(identity)

    def upsert_book_slot(self, quote):
        """First-writer-wins slot insert.

        Returns ``True`` only when the slot was empty.  An existing slot —
        even with different content — is preserved untouched.
        """
        cur = self.con.execute(
            "INSERT OR IGNORE INTO book_slots("
            "slot_hash,symbol,level,side,price,size,source,ts,"
            "issues,evidence,raw) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (self.slot_hash(quote), str(quote.get("symbol")),
             str(quote.get("level")), quote.get("side"),
             quote.get("price"), quote.get("size"), quote.get("source"),
             str(quote.get("ts") or ""),
             json.dumps(quote.get("issues", []), ensure_ascii=False),
             json.dumps(quote.get("evidence", []), ensure_ascii=False),
             json.dumps(quote.get("raw", {}), ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def all_book_slots(self):
        rows = self.con.execute(
            "SELECT symbol,level,side,price,size,source,ts,issues,evidence,raw "
            "FROM book_slots ORDER BY symbol, level, side").fetchall()
        out = []
        for symbol, level, side, price, size, source, ts, issues, evidence, raw in rows:
            out.append({"symbol": symbol, "level": _maybe_int(level),
                        "side": side, "price": price, "size": size,
                        "source": source, "ts": ts,
                        "issues": json.loads(issues),
                        "evidence": json.loads(evidence),
                        "raw": json.loads(raw)})
        return out

    def book_slot_count(self):
        return self.con.execute("SELECT COUNT(*) FROM book_slots").fetchone()[0]

    def get_book_slot(self, quote):
        """Return the durable owner of a quote's slot, or ``None`` if empty."""
        row = self.con.execute(
            "SELECT symbol,level,side,price,size,source,ts,issues,evidence,raw "
            "FROM book_slots WHERE slot_hash=?",
            (self.slot_hash(quote),)).fetchone()
        if row is None:
            return None
        symbol, level, side, price, size, source, ts, issues, evidence, raw = row
        return {"symbol": symbol, "level": _maybe_int(level), "side": side,
                "price": price, "size": size, "source": source, "ts": ts,
                "issues": json.loads(issues), "evidence": json.loads(evidence),
                "raw": json.loads(raw)}

    @staticmethod
    def audit_hash(kind, payload):
        blob = "%s|%s" % (kind, json.dumps(payload, sort_keys=True,
                                           ensure_ascii=False))
        return hashlib.sha256(blob.encode()).hexdigest()[:24]

    def audit_book_event(self, kind, payload, symbol=None, level=None,
                         side=None, ts=None):
        """Idempotent event audit; replaying the same event adds no row."""
        cur = self.con.execute(
            "INSERT OR IGNORE INTO book_audit("
            "audit_hash,kind,symbol,level,side,ts,payload) "
            "VALUES(?,?,?,?,?,?,?)",
            (self.audit_hash(kind, payload), kind,
             None if symbol is None else str(symbol),
             None if level is None else str(level),
             side, str(ts or ""),
             json.dumps(payload, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def all_audit_events(self):
        rows = self.con.execute(
            "SELECT kind,symbol,level,side,ts,payload FROM book_audit "
            "ORDER BY audit_hash").fetchall()
        return [{"kind": k, "symbol": s, "level": _maybe_int(l),
                 "side": side, "ts": ts, "payload": json.loads(p)}
                for k, s, l, side, ts, p in rows]

    def audit_event_count(self):
        return self.con.execute("SELECT COUNT(*) FROM book_audit").fetchone()[0]

    def audit_counts_by_kind(self):
        rows = self.con.execute(
            "SELECT kind, COUNT(*) FROM book_audit GROUP BY kind").fetchall()
        return dict(rows)

    # ------------------------------------------------------------------ #
    # Resumable run journal.
    # ------------------------------------------------------------------ #
    def journal_append(self, run_id, seq, record):
        cur = self.con.execute(
            "INSERT OR IGNORE INTO run_journal(run_id,seq,content_hash,payload) "
            "VALUES(?,?,?,?)",
            (str(run_id), int(seq), self.hash(record),
             json.dumps(record, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def journal_page(self, run_id, page, records):
        """Journal every canonical record of one fetched page.

        Idempotent per ``(run_id, content_hash)``: re-fetching a page on
        resume never duplicates rows or resets the ``applied`` flag.
        """
        existing = self.journal_seen(run_id)
        seq_base = self.journal_count(run_id)
        for record in records:
            h = self.hash(record)
            if h in existing:
                continue
            self.con.execute(
                "INSERT OR IGNORE INTO run_journal(run_id,seq,content_hash,payload) "
                "VALUES(?,?,?,?)",
                (str(run_id), seq_base, h,
                 json.dumps(record, ensure_ascii=False)))
            seq_base += 1
            existing.add(h)
        self.con.commit()

    def journal_seen(self, run_id):
        rows = self.con.execute(
            "SELECT content_hash FROM run_journal WHERE run_id=?",
            (str(run_id),)).fetchall()
        return {h for (h,) in rows}

    def journal_applied(self, run_id):
        """Hashes of records fully processed before an interrupt."""
        rows = self.con.execute(
            "SELECT content_hash FROM run_journal WHERE run_id=? AND applied=1",
            (str(run_id),)).fetchall()
        return {h for (h,) in rows}

    def journal_mark_applied(self, run_id, content_hash):
        self.con.execute(
            "UPDATE run_journal SET applied=1 "
            "WHERE run_id=? AND content_hash=?",
            (str(run_id), content_hash))
        self.con.commit()

    def journal_records(self, run_id):
        rows = self.con.execute(
            "SELECT payload FROM run_journal WHERE run_id=? ORDER BY seq",
            (str(run_id),)).fetchall()
        return [json.loads(p) for (p,) in rows]

    def journal_count(self, run_id):
        return self.con.execute(
            "SELECT COUNT(*) FROM run_journal WHERE run_id=?",
            (str(run_id),)).fetchone()[0]

    def reset(self):
        self.con.execute("DELETE FROM records")
        self.con.execute("DELETE FROM meta")
        self.con.execute("DELETE FROM book_slots")
        self.con.execute("DELETE FROM book_audit")
        self.con.execute("DELETE FROM run_journal")
        self.con.commit()


def _maybe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return value
