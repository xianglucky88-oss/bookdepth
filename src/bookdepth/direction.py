"""Reliable order-book direction normalization.

Quote sources do not agree on how the buy/sell direction is expressed:

* some carry an explicit side field (``"side": "bid"``, ``"direction": "卖"``);
* others hide the direction inside the level/field name
  (``"bid_price_1"``, ``"ask1"``, ``"卖一价"``).

The helpers in this module resolve the direction exactly once and return
canonical quotes whose ``side`` is one of :data:`BID`, :data:`ASK` or
:data:`UNKNOWN`.  Original numeric values are never negated or otherwise
rewritten — every coercion keeps the raw record alongside the parsed value,
and every conflict, missing value or unjudgeable direction is reported with
machine-readable ``issues`` and human-auditable ``evidence``.

The module is deliberately pure (no I/O) so the same rules are used by the
full crawler, the incremental collector and the historical replay path.
"""
from __future__ import annotations

import re
from typing import Any

BID = "bid"
ASK = "ask"
UNKNOWN = "unknown"

# Issue codes (stable identifiers, safe to assert on in tests).
ISSUE_MISSING_SIDE = "missing_side"
ISSUE_SIDE_CONFLICT = "side_conflict"
ISSUE_EXPLICIT_VS_EMBEDDED = "explicit_embedded_conflict"
ISSUE_NULL_PRICE = "null_price"
ISSUE_NULL_SIZE = "null_size"
ISSUE_BAD_NUMBER = "unparseable_number"
ISSUE_SLOT_BLOCKED = "slot_overwrite_blocked"
ISSUE_CROSSED = "crossed_book"
ISSUE_DUPLICATE_SLOT = "duplicate_slot"

# Vocabulary accepted by *explicit* side fields.  Single letters ("b"/"a")
# are only meaningful there, never as substrings of field names.
_BID_WORDS = frozenset({
    "bid", "bids", "b", "buy", "buys", "long", "bid-side", "bid side",
    "买", "买方", "买入", "买盘", "多", "多头",
})
_ASK_WORDS = frozenset({
    "ask", "asks", "a", "sell", "sells", "offer", "offers", "short",
    "ask-side", "ask side",
    "卖", "卖方", "卖出", "卖盘", "空", "空头",
})

_SIDE_KEYS = frozenset({
    "side", "direction", "dir", "bs", "bsflag", "type", "ordertype",
    "flag", "方向", "买卖", "买卖方向",
})
_LEVEL_KEYS = frozenset({
    "level", "depth", "tier", "rank", "quote_level", "档", "档位", "层次",
})
_SYMBOL_KEYS = frozenset({"symbol", "instrument", "code", "secid", "代码", "标的"})
_TS_KEYS = frozenset({"ts", "time", "timestamp", "datetime", "时间"})

_CN_NUM = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
           "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
_NULL_WORDS = frozenset({"", "null", "none", "nan", "n/a", "na", "-", "--"})


# --------------------------------------------------------------------------- #
# Direction inference.
# --------------------------------------------------------------------------- #
def normalize_side(value: Any) -> str | None:
    """Map an explicit side value to ``bid``/``ask``; ``None`` if unknown."""
    if value is None:
        return None
    token = str(value).strip().lower()
    if token in _BID_WORDS:
        return BID
    if token in _ASK_WORDS:
        return ASK
    return None


def direction_in_key(key: Any) -> str | None:
    """Infer a direction from a field name.

    Returns ``BID``/``ASK``/``UNKNOWN`` (when the name carries both) or
    ``None`` when the name is not directional at all.
    """
    k = str(key).lower()
    has_bid = ("bid" in k or "buy" in k) or ("买" in k)
    has_ask = (("ask" in k or "offer" in k or "sell" in k)) or ("卖" in k)
    if has_bid and has_ask:
        return UNKNOWN
    if has_bid:
        return BID
    if has_ask:
        return ASK
    return None


def level_from_key(key: Any) -> int | None:
    """Extract a book level from digits or Chinese numerals in a name."""
    k = str(key)
    m = re.search(r"\d+", k)
    if m:
        return int(m.group())
    for ch, num in _CN_NUM.items():
        if ch in k:
            return num
    return None


def _field_role(key: Any) -> str | None:
    k = str(key).lower()
    if any(tok in k for tok in ("price", "px", "价", "价格")):
        return "price"
    if any(tok in k for tok in ("size", "qty", "quantity", "volume",
                                "amount", "量", "额", "手")):
        return "size"
    return None


def to_number(value: Any) -> tuple[float | None, bool]:
    """Coerce a value to float without ever flipping its sign.

    Returns ``(number, ok)``.  ``ok`` is ``False`` for null/blank/unparseable
    inputs, in which case ``number`` is ``None``.  The original representation
    stays available in the quote's ``raw`` record.
    """
    if value is None or isinstance(value, bool):
        return None, False
    if isinstance(value, (int, float)):
        return float(value), True
    token = str(value).strip()
    if token.lower() in _NULL_WORDS:
        return None, False
    try:
        # float() preserves an explicit leading minus sign; nothing here
        # negates or otherwise rewrites the source value.
        return float(token), True
    except ValueError:
        return None, False


# --------------------------------------------------------------------------- #
# Record -> canonical quotes.
# --------------------------------------------------------------------------- #
def _context(record: dict) -> dict:
    ctx = {"symbol": None, "level": None, "ts": None}
    for k, v in record.items():
        kl = str(k).lower()
        if kl in _SYMBOL_KEYS and ctx["symbol"] is None:
            ctx["symbol"] = v
        elif kl in _TS_KEYS and ctx["ts"] is None:
            ctx["ts"] = v
        elif kl in _LEVEL_KEYS:
            lvl, ok = to_number(v)
            ctx["level"] = int(lvl) if ok and lvl is not None else level_from_key(k)
    return ctx


def _base_quote(record: dict, ctx: dict, side: str, level, source: str,
                issues: list[str], evidence: list[str]) -> dict:
    return {
        "symbol": ctx.get("symbol"),
        "level": level,
        "side": side,
        "price": None,
        "size": None,
        "ts": ctx.get("ts"),
        "source": source,
        "issues": list(issues),
        "evidence": list(evidence),
        "raw": record,
    }


def _explicit_sides(record: dict) -> list[tuple[str, str]]:
    """All (key, normalized side) pairs carried by explicit side fields."""
    out = []
    for k, v in record.items():
        if str(k).lower() in _SIDE_KEYS:
            side = normalize_side(v)
            if side is not None:
                out.append((str(k), side))
    return out


def extract_quotes(record: dict) -> list[dict]:
    """Turn one raw source record into one or more canonical quotes.

    A record may be:

    * a single-level quote with an explicit side field;
    * a wide row with direction-bearing field names (expanded into one quote
      per ``(side, level)`` group);
    * unjudgeable — still returned with ``side == "unknown"`` and an issue.
    """
    if not isinstance(record, dict):
        return []
    ctx = _context(record)
    explicit = _explicit_sides(record)
    explicit_sides = {side for _k, side in explicit}
    explicit_evidence = ["field:%s=%s" % (k, record.get(k)) for k, _s in explicit]

    # Phase A: direction hidden in field names.
    groups: dict[tuple[str, Any], dict] = {}
    contradictory_keys: list[str] = []
    for k, v in record.items():
        direction = direction_in_key(k)
        if direction is None:
            continue
        if direction == UNKNOWN:
            contradictory_keys.append(str(k))
            continue
        role = _field_role(k)
        if role is None:
            # A bare directional numeric cell (e.g. {"bid1": 100.2}).
            num, ok = to_number(v)
            role = "price" if ok else None
        if role is None:
            continue
        lvl = level_from_key(k)
        if lvl is None:
            lvl = ctx.get("level")
        slot = groups.setdefault((direction, lvl), {"price": None, "size": None,
                                                     "keys": []})
        slot[role] = v
        slot["keys"].append(str(k))

    quotes: list[dict] = []

    if groups:
        for (direction, lvl), slot in sorted(groups.items(),
                                             key=lambda item: (item[0][1] is None,
                                                               item[0][1] or 0,
                                                               item[0][0])):
            issues, evidence = [], ["name:%s" % ",".join(slot["keys"])]
            side = direction
            if explicit_sides and direction not in explicit_sides:
                # Explicit field and field name claim opposite directions.
                issues.append(ISSUE_EXPLICIT_VS_EMBEDDED)
                evidence.extend(explicit_evidence)
                side = UNKNOWN
            q = _base_quote(record, ctx, side, lvl, "embedded", issues, evidence)
            _fill_metrics(q, slot["price"], slot["size"])
            quotes.append(q)
        for bad_key in contradictory_keys:
            q = _base_quote(record, ctx, UNKNOWN, ctx.get("level"), "embedded",
                            [ISSUE_SIDE_CONFLICT],
                            ["name:%s carries both directions" % bad_key])
            _fill_metrics(q, record.get(bad_key), None)
            quotes.append(q)
        return quotes

    # Phase B: a single-level quote; direction must come from an explicit field.
    issues, evidence = [], list(explicit_evidence)
    if len(explicit_sides) > 1:
        issues.append(ISSUE_SIDE_CONFLICT)
        side = UNKNOWN
    elif len(explicit_sides) == 1:
        side = next(iter(explicit_sides))
    else:
        issues.append(ISSUE_MISSING_SIDE)
        side = UNKNOWN

    price_val = size_val = None
    for k, v in record.items():
        role = _field_role(k)
        if role == "price":
            price_val = v
        elif role == "size":
            size_val = v
    q = _base_quote(record, ctx, side, ctx.get("level"),
                    "explicit" if explicit else "unknown", issues, evidence)
    _fill_metrics(q, price_val, size_val)
    return [q]


def _fill_metrics(quote: dict, price_val: Any, size_val: Any) -> None:
    if price_val is not None:
        num, ok = to_number(price_val)
        if not ok:
            quote["issues"].append(
                ISSUE_NULL_PRICE if str(price_val).strip().lower() in _NULL_WORDS
                or price_val is None else ISSUE_BAD_NUMBER)
        quote["price"] = num
    else:
        quote["issues"].append(ISSUE_NULL_PRICE)
    if size_val is not None:
        num, ok = to_number(size_val)
        if not ok:
            quote["issues"].append(
                ISSUE_NULL_SIZE if str(size_val).strip().lower() in _NULL_WORDS
                or size_val is None else ISSUE_BAD_NUMBER)
        quote["size"] = num
    else:
        quote["issues"].append(ISSUE_NULL_SIZE)


# --------------------------------------------------------------------------- #
# Merging into a book.
# --------------------------------------------------------------------------- #
def new_book_state() -> dict:
    """Mutable accumulator used by :func:`merge_quote` / :func:`finalize_book`."""
    return {"groups": {}, "conflicts": [], "unresolved": []}


def merge_quote(state: dict, quote: dict) -> dict:
    """Merge one canonical quote into the book state.

    A later arrival can never silently overwrite an earlier quote:

    * opposite directions fill opposite slots of the same level;
    * a second quote for an occupied bid/ask slot is blocked and recorded as
      a conflict, regardless of arrival order;
    * unknown-direction quotes are parked unresolved and never auto-assigned.
    """
    key = (quote.get("symbol"), quote.get("level"))
    group = state["groups"].setdefault(key, {"bid": None, "ask": None,
                                             "issues": []})
    side = quote.get("side")
    if side not in (BID, ASK):
        state["unresolved"].append(quote)
        group["issues"].append(ISSUE_MISSING_SIDE)
        return state

    occupied = group[side]
    if occupied is None:
        group[side] = quote
        return state

    same_price = occupied.get("price") == quote.get("price")
    same_size = occupied.get("size") == quote.get("size")
    if same_price and same_size:
        group["issues"].append(ISSUE_DUPLICATE_SLOT)
        return state

    # Conflicting content for the same slot: keep the first quote, log the
    # incoming one with both raw payloads for investigation.
    conflict = {
        "symbol": quote.get("symbol"),
        "level": quote.get("level"),
        "side": side,
        "kind": ISSUE_SLOT_BLOCKED,
        "existing": occupied,
        "incoming": quote,
        "evidence": ["existing_source:%s" % occupied.get("source"),
                     "incoming_source:%s" % quote.get("source")]
                     + list(quote.get("evidence", [])),
    }
    state["conflicts"].append(conflict)
    group["issues"].append(ISSUE_SLOT_BLOCKED)
    return state


def finalize_book(state: dict) -> dict:
    """Produce the stable, order-independent output view of a book."""
    levels = []
    for (symbol, lvl), group in sorted(state["groups"].items(),
                                       key=_group_sort_key):
        bid, ask = group["bid"], group["ask"]
        bid_price = bid.get("price") if bid else None
        ask_price = ask.get("price") if ask else None
        issues = list(group["issues"])
        spread = None
        valid_order = 0
        crossed = False
        if bid_price is not None and ask_price is not None:
            spread = round(ask_price - bid_price, 6)
            valid_order = int(bid_price <= ask_price)
            crossed = bid_price > ask_price
            if crossed:
                issues.append(ISSUE_CROSSED)
        levels.append({
            "symbol": symbol,
            "level": lvl,
            "bid": _slot_view(bid),
            "ask": _slot_view(ask),
            "bid_price": bid_price,
            "ask_price": ask_price,
            "spread": spread,
            "spread_available": spread is not None,
            "valid_order": valid_order,
            "crossed": crossed,
            "issues": sorted(set(issues)),
        })
    return {
        "levels": levels,
        "conflicts": list(state["conflicts"]),
        "unresolved": list(state["unresolved"]),
    }


def _group_sort_key(item):
    (symbol, lvl) = item[0]
    return (str(symbol or ""), lvl is None, lvl if lvl is not None else 0)


def _slot_view(quote: dict | None) -> dict | None:
    if quote is None:
        return None
    return {"price": quote.get("price"), "size": quote.get("size"),
            "source": quote.get("source"),
            "issues": list(quote.get("issues", [])),
            "evidence": list(quote.get("evidence", []))}


def build_book(quotes) -> dict:
    """Convenience: build the final book view from an iterable of quotes."""
    state = new_book_state()
    for quote in quotes:
        merge_quote(state, quote)
    return finalize_book(state)


def records_to_book(records) -> dict:
    """End-to-end helper: raw records -> canonical book view."""
    state = new_book_state()
    for record in records:
        for quote in extract_quotes(record):
            merge_quote(state, quote)
    return finalize_book(state)
