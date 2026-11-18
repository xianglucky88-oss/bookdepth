
"""Public orchestration entry point."""
from . import framework


def run(mode="full", con=None):
    from .config import SPEC
    if SPEC.engine == "rpa":
        return framework.run_rpa(con=con)
    return framework.run(mode=mode, con=con)


def run_book(mode="full", con=None, **kwargs):
    """Direction-aware order-book pipeline (see framework.run_book)."""
    return framework.run_book(mode=mode, con=con, **kwargs)


def replay_book(records, con=None, run_id="book-replay"):
    return framework.replay_book(records, con=con, run_id=run_id)
