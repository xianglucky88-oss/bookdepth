
def process(record):
    try:
        v = float(record.get("value") or 0)
    except (TypeError, ValueError):
        v = 0.0
    bid = round(v * 0.98, 4)
    ask = round(v * 1.02, 4)
    return {"bid": bid, "ask": ask, "spread": round(ask - bid, 4),
            "valid_order": int(bid <= ask)}
