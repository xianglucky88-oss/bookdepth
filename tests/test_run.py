import sqlite3

from bookdepth.config import SPEC
from bookdepth.pipeline import run
from bookdepth.store import Store

def test_pipeline_returns_a_status():
    con = sqlite3.connect(":memory:")
    Store(con)
    try:
        result = run(con=con)
    finally:
        con.close()
    assert isinstance(result, dict)
    assert result.get("engine") == SPEC.engine
    assert result
