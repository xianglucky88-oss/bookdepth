PY=python3
run:
	PYTHONPATH=src:. $(PY) -m bookdepth.cli
run_inc:
	PYTHONPATH=src:. $(PY) -m bookdepth.cli --incremental
test:
	PYTHONPATH=src:. $(PY) -m pytest -q
mock:
	PYTHONPATH=src:. $(PY) -m uvicorn mockapp.main:app --port 8000
submit:
	PYTHONPATH=src:. $(PY) -m bookdepth.manage submit --mode full
work:
	PYTHONPATH=src:. $(PY) -m bookdepth.manage work --once
clean:
	rm -rf .pytest_cache **/__pycache__ *.db *.db-wal *.db-shm
