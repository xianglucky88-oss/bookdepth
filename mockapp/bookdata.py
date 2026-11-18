"""Deterministic order-book fixture rows for /book.

The rows deliberately cover the direction edge cases that the collection
pipeline must handle: explicit side fields, direction hidden in field names
(English and Chinese), ask arriving before bid, shuffled levels, null and
unparseable numbers, explicit/embedded contradictions, missing direction,
a crossed book and duplicated page content.
"""
from __future__ import annotations

T1 = "2030-02-01T00:00:00Z"
T2 = "2030-02-02T00:00:00Z"
T3 = "2030-02-03T00:00:00Z"
T4 = "2030-02-04T00:00:00Z"
T9 = "2030-02-09T00:00:00Z"


def base_rows():
    rows = [
        # Page 1: ask before bid ("先卖后买"), and level 3 arriving before
        # level 2 ("档位乱序").
        {"id": "B-001", "symbol": "AAA", "level": 1, "side": "ask",
         "price": 101.0, "size": 7, "ts": T1},
        {"id": "B-002", "symbol": "AAA", "level": 1, "side": "bid",
         "price": 100.0, "size": 5, "ts": T1},
        {"id": "B-003", "symbol": "AAA", "level": 3,
         "bid_price_3": 98.0, "bid_size_3": 4,
         "ask_price_3": 103.0, "ask_size_3": 6, "ts": T1},
        # Page 2: level 2 fills in late; a later, contradictory bid for an
        # occupied slot must be blocked rather than overwrite; Chinese names.
        {"id": "B-004", "symbol": "AAA", "level": 2,
         "bid_price_2": 99.0, "bid_size_2": 5,
         "ask_price_2": 102.0, "ask_size_2": None, "ts": T2},
        {"id": "B-005", "symbol": "AAA", "level": 1, "side": "bid",
         "price": 99.5, "size": 9, "ts": T2},
        {"id": "B-006", "symbol": "BBB", "level": 1,
         "买一价": 50.0, "买一量": 3, "卖一价": 50.5, "卖一量": 2,
         "ts": T2},
        # Page 3: explicit field says bid while the field names say ask;
        # a row with no direction information at all; start of a crossed book.
        {"id": "B-007", "symbol": "CCC", "level": 1, "side": "bid",
         "ask_price_1": 200.0, "ask_size_1": 3, "ts": T3},
        {"id": "B-008", "symbol": "CCC", "level": 2,
         "price": 10.0, "size": 1, "ts": T3},
        {"id": "B-009", "symbol": "DDD", "level": 1, "side": "bid",
         "price": 60.0, "size": 3, "ts": T3},
        # Page 4: the crossed ask arrives late; duplicated page content (an
        # exact repeat of the level-2 wide row, as re-sent pages produce);
        # an unparseable price that must keep its raw representation.
        {"id": "B-010", "symbol": "DDD", "level": 1, "side": "ask",
         "price": 59.0, "size": 2, "ts": T4},
        {"id": "B-004", "symbol": "AAA", "level": 2,
         "bid_price_2": 99.0, "bid_size_2": 5,
         "ask_price_2": 102.0, "ask_size_2": None, "ts": T2},
        {"id": "B-012", "symbol": "EEE", "level": 1, "side": "bid",
         "price": "abc", "size": 4, "ts": T4},
    ]
    return rows


def extra_rows():
    """Rows exposed only after /book/refresh; timestamps are strictly newer."""
    return [
        {"id": "B-X001", "symbol": "AAA", "level": 4, "side": "bid",
         "price": 97.0, "size": 8, "ts": T9},
        {"id": "B-X002", "symbol": "AAA", "level": 4, "side": "ask",
         "price": 104.0, "size": 9, "ts": T9},
    ]
