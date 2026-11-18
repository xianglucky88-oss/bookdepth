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

## Run management (single machine)

`python -m bookdepth.manage` wraps the same collection pipeline with scheduling, mutual exclusion, cancellation and audit, so timed (due) tasks and manual catch-up runs can be initiated, cancelled and queried:

```text
PYTHONPATH=src:. python -m bookdepth.manage submit --mode full          # manual catch-up run
PYTHONPATH=src:. python -m bookdepth.manage schedule add --name inc --mode incremental --interval 300
PYTHONPATH=src:. python -m bookdepth.manage work --once                 # claim + execute one due run
PYTHONPATH=src:. python -m bookdepth.manage work                        # loop: tick schedules, reap expired leases, execute
PYTHONPATH=src:. python -m bookdepth.manage cancel --run 3 --reason "operator stop"
PYTHONPATH=src:. python -m bookdepth.manage status --run 3              # state, events, denial reasons
PYTHONPATH=src:. python -m bookdepth.manage list
```

Behaviour notes:

- One data resource is never executed in parallel: claims take a lease with a fencing epoch, the oldest due run is served first, and a busy resource never blocks runs for other resources.
- Expired leases are requeued by `reap` (also run by `work`) with an overdue event and a backoff `retry_at`; late results from stale or reappearing executors are rejected, and cancel/complete races resolve to whichever committed first, so the final record stays trustworthy even if a controller or executor dies mid-run.
- `status` shows whether an unfinished run can still continue (`can_continue`), when it can be attempted again (`next_attempt_at`), and why it did not get execution rights (`last_denial`).
- Cancelling a run does not retract quotes already written to the business tables; it only settles how the run itself is recorded.
- Management state lives in extra tables inside the same SQLite file; pass `--db PATH` to use a separate file.
