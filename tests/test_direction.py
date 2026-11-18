"""Unit tests for reliable bid/ask direction resolution."""
import pytest

from bookdepth.direction import (
    BID, ASK, UNKNOWN,
    normalize_side, direction_in_key, level_from_key, to_number,
    extract_quotes, merge_quote, new_book_state, finalize_book,
    build_book, records_to_book,
    ISSUE_MISSING_SIDE, ISSUE_SIDE_CONFLICT, ISSUE_EXPLICIT_VS_EMBEDDED,
    ISSUE_NULL_PRICE, ISSUE_NULL_SIZE, ISSUE_BAD_NUMBER,
    ISSUE_SLOT_BLOCKED, ISSUE_CROSSED, ISSUE_DUPLICATE_SLOT,
)


# --------------------------------------------------------------------------- #
# Vocabulary and key parsing.
# --------------------------------------------------------------------------- #
def test_normalize_side_accepts_english_and_chinese_vocabulary():
    for token in ("bid", "BID", " buy ", "b", "买", "买入", "多头"):
        assert normalize_side(token) == BID
    for token in ("ask", "Ask", "sell", "offer", "a", "卖", "卖出", "空头"):
        assert normalize_side(token) == ASK
    assert normalize_side(None) is None
    assert normalize_side("") is None
    assert normalize_side("weird") is None


def test_direction_in_key_handles_names_english_and_chinese():
    assert direction_in_key("bid_price_1") == BID
    assert direction_in_key("BuyQty") == BID
    assert direction_in_key("买一价") == BID
    assert direction_in_key("ask_size_2") == ASK
    assert direction_in_key("sellPx") == ASK
    assert direction_in_key("卖一量") == ASK
    assert direction_in_key("price") is None
    # A name carrying both directions is ambiguous, not silently guessed.
    assert direction_in_key("bid_ask_spread") == UNKNOWN


def test_level_from_key_digits_and_chinese_numerals():
    assert level_from_key("bid_price_3") == 3
    assert level_from_key("卖二价") == 2
    assert level_from_key("price") is None


@pytest.mark.parametrize("raw,expected,ok", [
    (100.25, 100.25, True),
    ("100.25", 100.25, True),
    (-7.5, -7.5, True),          # sign is preserved, never flipped
    ("-7.5", -7.5, True),
    (0, 0.0, True),
    (None, None, False),
    ("", None, False),
    ("null", None, False),
    ("abc", None, False),
])
def test_to_number_never_flips_sign(raw, expected, ok):
    value, parsed = to_number(raw)
    assert parsed is ok
    assert value == expected


# --------------------------------------------------------------------------- #
# Record extraction: explicit sides, embedded names, nulls and conflicts.
# --------------------------------------------------------------------------- #
def test_explicit_side_quote_carries_evidence_and_raw():
    rec = {"id": "r1", "symbol": "AAA", "level": 1, "side": "bid",
           "price": 100.0, "size": 5, "ts": "t1"}
    quotes = extract_quotes(rec)
    assert len(quotes) == 1
    q = quotes[0]
    assert q["side"] == BID and q["price"] == 100.0 and q["size"] == 5.0
    assert q["source"] == "explicit"
    assert any("field:side" in e for e in q["evidence"])
    assert q["raw"] == rec


def test_embedded_wide_row_expands_into_canonical_quotes():
    rec = {"symbol": "AAA", "bid_price_2": 99.0, "bid_size_2": 5,
           "ask_price_2": 102.0, "ask_size_2": None}
    quotes = extract_quotes(rec)
    by_side = {q["side"]: q for q in quotes}
    assert set(by_side) == {BID, ASK}
    assert by_side[BID]["level"] == 2 and by_side[BID]["price"] == 99.0
    assert by_side[ASK]["price"] == 102.0
    assert ISSUE_NULL_SIZE in by_side[ASK]["issues"]


def test_null_and_unparseable_prices_are_auditable_not_zeroed():
    rec = {"symbol": "EEE", "level": 1, "side": "bid",
           "price": "abc", "size": None}
    q = extract_quotes(rec)[0]
    assert q["price"] is None
    assert ISSUE_BAD_NUMBER in q["issues"]
    assert ISSUE_NULL_SIZE in q["issues"]
    # The original representation is retained untouched.
    assert q["raw"]["price"] == "abc"


def test_missing_side_is_unknown_with_issue():
    rec = {"symbol": "CCC", "level": 2, "price": 10.0, "size": 1}
    q = extract_quotes(rec)[0]
    assert q["side"] == UNKNOWN
    assert ISSUE_MISSING_SIDE in q["issues"]


def test_conflicting_explicit_side_fields_become_unknown():
    rec = {"symbol": "X", "level": 1, "side": "bid", "direction": "ask",
           "price": 10, "size": 1}
    q = extract_quotes(rec)[0]
    assert q["side"] == UNKNOWN
    assert ISSUE_SIDE_CONFLICT in q["issues"]


def test_explicit_side_contradicting_embedded_names_is_blocked():
    rec = {"symbol": "CCC", "level": 1, "side": "bid",
           "ask_price_1": 200.0, "ask_size_1": 3}
    q = extract_quotes(rec)[0]
    assert q["side"] == UNKNOWN
    assert ISSUE_EXPLICIT_VS_EMBEDDED in q["issues"]
    # Both claims are recorded for later investigation.
    assert any("field:side" in e for e in q["evidence"])
    assert any("name:" in e for e in q["evidence"])


def test_bare_directional_cell_is_treated_as_price():
    rec = {"symbol": "Z", "bid1": 100.2}
    q = extract_quotes(rec)[0]
    assert q["side"] == BID and q["level"] == 1 and q["price"] == 100.2


def test_missing_price_field_is_reported_not_invented():
    rec = {"symbol": "Z", "level": 1, "side": "ask", "size": 4}
    q = extract_quotes(rec)[0]
    assert q["price"] is None
    assert ISSUE_NULL_PRICE in q["issues"]


# --------------------------------------------------------------------------- #
# Merging: arrival order independence and first-writer-wins.
# --------------------------------------------------------------------------- #
def _q(symbol, level, side, price, size=1, source="explicit"):
    return {"symbol": symbol, "level": level, "side": side,
            "price": price, "size": size, "ts": "t", "source": source,
            "issues": [], "evidence": ["test"], "raw": {}}


def test_opposite_directions_fill_distinct_slots():
    state = new_book_state()
    merge_quote(state, _q("AAA", 1, ASK, 101.0))   # ask arrives first
    merge_quote(state, _q("AAA", 1, BID, 100.0))
    view = finalize_book(state)
    lvl = view["levels"][0]
    assert lvl["bid_price"] == 100.0 and lvl["ask_price"] == 101.0
    assert lvl["spread"] == 1.0 and lvl["valid_order"] == 1


def test_later_quote_can_never_overwrite_occupied_slot():
    state = new_book_state()
    merge_quote(state, _q("AAA", 1, BID, 100.0))
    merge_quote(state, _q("AAA", 1, BID, 99.5, source="late"))
    view = finalize_book(state)
    lvl = view["levels"][0]
    assert lvl["bid_price"] == 100.0                # first value kept
    assert ISSUE_SLOT_BLOCKED in lvl["issues"]
    assert len(view["conflicts"]) == 1
    conflict = view["conflicts"][0]
    assert conflict["existing"]["price"] == 100.0
    assert conflict["incoming"]["price"] == 99.5


def test_first_writer_wins_is_order_independent():
    # The same two quotes in reverse arrival order must resolve differently
    # only in which one is kept — never in a silent overwrite.
    state_b = new_book_state()
    merge_quote(state_b, _q("AAA", 1, BID, 99.5, source="late"))
    merge_quote(state_b, _q("AAA", 1, BID, 100.0))
    view_b = finalize_book(state_b)
    assert view_b["levels"][0]["bid_price"] == 99.5
    assert ISSUE_SLOT_BLOCKED in view_b["levels"][0]["issues"]


def test_identical_duplicate_slot_is_flagged_but_not_a_conflict():
    state = new_book_state()
    merge_quote(state, _q("AAA", 1, BID, 100.0, size=5))
    merge_quote(state, _q("AAA", 1, BID, 100.0, size=5))
    view = finalize_book(state)
    assert ISSUE_DUPLICATE_SLOT in view["levels"][0]["issues"]
    assert view["conflicts"] == []


def test_unknown_direction_quotes_are_parked_not_guessed():
    state = new_book_state()
    merge_quote(state, _q("CCC", 2, UNKNOWN, 10.0))
    view = finalize_book(state)
    assert view["levels"][0]["bid"] is None and view["levels"][0]["ask"] is None
    assert len(view["unresolved"]) == 1


def test_crossed_book_is_flagged_without_repairing_numbers():
    state = new_book_state()
    merge_quote(state, _q("DDD", 1, BID, 60.0))
    merge_quote(state, _q("DDD", 1, ASK, 59.0))
    view = finalize_book(state)
    lvl = view["levels"][0]
    assert lvl["crossed"] is True
    assert lvl["spread"] == -1.0                     # raw numbers untouched
    assert lvl["valid_order"] == 0
    assert ISSUE_CROSSED in lvl["issues"]


def test_shuffled_levels_and_ask_first_produce_stable_book():
    records = [
        {"symbol": "AAA", "level": 3, "side": "ask", "price": 103.0, "size": 6},
        {"symbol": "AAA", "level": 1, "side": "ask", "price": 101.0, "size": 7},
        {"symbol": "AAA", "level": 1, "side": "bid", "price": 100.0, "size": 5},
        {"symbol": "AAA", "level": 3, "side": "bid", "price": 98.0, "size": 4},
        {"symbol": "AAA", "level": 2, "side": "bid", "price": 99.0, "size": 5},
        {"symbol": "AAA", "level": 2, "side": "ask", "price": 102.0, "size": 3},
    ]
    reversed_view = records_to_book(list(reversed(records)))
    view = records_to_book(records)
    assert [l["level"] for l in view["levels"]] == [1, 2, 3]
    assert [l["level"] for l in reversed_view["levels"]] == [1, 2, 3]
    assert ([(l["bid_price"], l["ask_price"], l["spread"]) for l in view["levels"]]
            == [(l["bid_price"], l["ask_price"], l["spread"])
                for l in reversed_view["levels"]])
    assert [l["spread"] for l in view["levels"]] == [1.0, 3.0, 5.0]


def test_build_book_spread_none_when_a_side_is_missing():
    view = build_book([_q("Q", 1, BID, 10.0)])
    lvl = view["levels"][0]
    assert lvl["spread"] is None and lvl["spread_available"] is False
