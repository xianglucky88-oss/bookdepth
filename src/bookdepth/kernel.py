"""Record-level transforms.

``process`` is the legacy synthetic transform and is intentionally left
unchanged so existing stored payloads stay identical.  Direction-aware order
book handling lives in :mod:`bookdepth.direction` and is re-exported here so
the kernel remains the single public processing entry point.
"""
from .direction import (  # noqa: F401
    BID, ASK, UNKNOWN,
    build_book, extract_quotes, merge_quote, new_book_state, finalize_book,
    normalize_side, direction_in_key, to_number, records_to_book,
)


def process(record):
    try:
        v = float(record.get("value") or 0)
    except (TypeError, ValueError):
        v = 0.0
    bid = round(v * 0.98, 4)
    ask = round(v * 1.02, 4)
    return {"bid": bid, "ask": ask, "spread": round(ask - bid, 4),
            "valid_order": int(bid <= ask)}


def book_summary(records):
    """Return canonical levels plus stable, order-independent spread stats."""
    view = records_to_book(records)
    spreads = sorted(level["spread"] for level in view["levels"]
                     if level["spread_available"])
    view["spread_min"] = spreads[0] if spreads else None
    view["spread_max"] = spreads[-1] if spreads else None
    view["acceptable"] = bool(spreads) and min(spreads) >= 0
    return view
