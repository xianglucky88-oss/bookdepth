
from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "bookdepth.db"

SPEC = SimpleNamespace(
    slug="bookdepth",
    fmt="json",
    engine="crawl",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts", "bid", "ask"],
    page_size=4,
    book_page_size=3,
    int_fields=["value"],
    ts_field="ts",
)
