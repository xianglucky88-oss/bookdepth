# bookdepth

A small offline data-processing service with a local HTTP fixture and SQLite storage. It is self-contained and does not require network access.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/bookdepth`. The fixture service supports the source formats used by the package, and the command-line entry point writes results to the local SQLite database. Re-running commands is safe for an existing database.

### Order-book direction normalization

Quote sources express the buy/sell direction inconsistently: some carry an
explicit side field (`side`, `direction`, 中文 `买卖`/`方向`), others hide it
inside the level name (`bid_price_2`, `ask1`, `卖一价`). The direction layer
(`bookdepth.direction`, exposed through `bookdepth.kernel`) resolves the
direction exactly once into canonical quotes whose `side` is one of
`bid`, `ask` or `unknown`:

* original numeric values are never negated or rewritten — the raw record is
  retained on every quote, and unparseable/null values stay `NULL` with an
  audit issue instead of being silently zeroed;
* a later, contradictory quote can never overwrite an occupied bid/ask slot
  (first-writer-wins). Opposite directions fill opposite slots of the same
  level, so ask-before-bid and shuffled levels produce the same book;
* conflicts, missing/ambiguous directions, nulls and crossed books are
  recorded in the `book_audit` table with the raw evidence needed to trace
  them; nothing is guessed.

```text
python -m bookdepth.cli --book                    # full book collection
python -m bookdepth.cli --book --incremental     # only rows past the watermark
python -m bookdepth.cli --book --fail-after=N    # simulate an interrupt
python -m bookdepth.cli --book --resume          # resume from the checkpoint
```

Durable tables (`book_slots`, `book_audit`, `run_journal`) live alongside the
original `records`/`meta` tables. Request pacing, exponential-backoff retries,
the backfill watermark and content-hash duplicate detection reuse the original
mechanisms unchanged, and databases from earlier versions are migrated in
place.

