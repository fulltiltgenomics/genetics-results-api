# Genetics Results API

API to serve human genetics association results and annotations

This is deployed as part of FinnGenie AI assistant (see [https://github.com/fulltiltgenomics/genetics-results-suite](https://github.com/fulltiltgenomics/genetics-results-suite))

A deployment with publicly available data is not yet available

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

Install requirements

```bash
uv sync
```

Update the data paths in the profile modules under [app/config/](app/config/) and the dataset
registry `configs/datasets.yaml` to point to your data files. The
active profile is chosen with `CONFIG_PROFILE` (default `daly`) and the registry path with
`DATASETS_CONFIG_PATH` (default `./configs/datasets.yaml`)

`configs/datasets.yaml` is **not committed to this repo** — it is generated. The canonical
file lives in the `genetics-results-suite` repo; create the local copy with
`../genetics-results-suite/scripts/sync-datasets.sh`, or point `DATASETS_CONFIG_PATH` at
your own. The server and the test suite both fail to start without it.

## Run the server

```bash
uv run python run_server.py 8081
```

For local development with auto-reload on code changes, set `RELOAD=1` (off by
default so production starts as a single warmed process):

```bash
RELOAD=1 uv run python run_server.py 8081
```

## API docs

Once running, open http://localhost:8081/api/v1/docs to see available endpoints

## Run tests against running server

```bash
uv pip install --system -r pyproject.toml --extra dev
SERVER_URL=http://localhost:8081 tests/run_tests.sh
```

## Linting

```bash
ruff check                      # the whole repo
scripts/lint-staged.sh          # only what is staged — what the pre-commit hook runs
scripts/lint-staged.sh --all    # the whole repo, via the same resolution logic
```

Run `scripts/install-git-hooks.sh` once per clone. It wires `core.hooksPath`, which no
clone carries, so that `pre-commit` runs both `scripts/check-doc-drift.sh` (warns) and
`scripts/lint-staged.sh` (**blocks the commit** on a finding). `core.hooksPath` is shared
across worktrees, so that one run covers every worktree too.

The gate looks for ruff in this checkout's `.venv`, then the **main checkout's** (a
worktree has none of its own), then `PATH`, then `uvx` — and fails the commit if it finds
none, rather than passing it unchecked. `git commit --no-verify` is the deliberate bypass.

## License

MIT
