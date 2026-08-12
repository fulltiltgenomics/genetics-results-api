====


QUALITY CODING RULES


# Code changes

1. If you find errors or suggestions in code which are not DIRECTLY related to user's current request, never change it without asking first.
2. Before suggesting changes to files, always assume user might have changed the file since your last read and consider reading the file again.


# Security

1. Never commit sensitive files (.env, credentials, API keys)
2. Use environment variables for API keys and credentials
3. Keep API keys and credentials out of logs and output
4. The API is read-only by design - never add write operations to data sources


# Project Specifications

1. Project documentation is maintained in files in `docs/` folder.
2. `docs/project-spec.md` is an overview of project purpose, structure and logic.
3. Create other files under `docs/` if necessary.
4. Maintain `docs/project-spec.md` and any other generated files to be up to date with the project.
5. Reread `docs/project-spec.md` often and whenever you need to refresh your context with what the project is about and implementation logic.
6. This should often be your first step in understanding a task.


# Documentation ownership

Changing a path on the left makes the doc on the right wrong until it is updated in
the same commit. `scripts/check-doc-drift.sh` warns (never blocks) on commits that
violate this; it runs from the `pre-commit` hook.

| changed path | doc to update | what to check |
|---|---|---|
| `app/routers/**` | `docs/project-spec.md` | the API Endpoints table — one row per router registered in `app/server.py` |
| `app/config/**` (including `profiles/**`) | `docs/project-spec.md` | per-product config, which datasets/resources/files exist, profile config keys |
| `app/core/auth.py` | `docs/project-spec.md` | the Authentication section: accepted credential order, env vars, the `@is_public` list |
| `app/services/startup_checks.py` | `docs/project-spec.md` | the enumerated data-file families, what is intentionally excluded, the file counts |
| `pyproject.toml` | `docs/project-spec.md` | Tech Stack: Python version, dependency claims, why `PyJWT` stays pinned |

A doc is stale the moment it *enumerates* something the code no longer matches.
Counts and lists rot silently — endpoint tables, dataset lists, verified-file
families — so re-derive them from the code rather than trusting them.


# Cross-repo documentation

`genetics-results-suite` is the spec of record for the suite as a whole; this repo
documents only itself. Adding or changing an **API route or dataset** here therefore
also requires updating that repo's `docs/project-spec.md` and `docs/adding-datasets.md`,
not just the docs here — this repo's own docs cannot detect that class of drift.


# Software Development Behavior Guidelines

1. Don't guess and do things which you are not certain about. Ask the user instead.
2. Don't add or modify code unrelated to the specific request and context at the moment.
3. In interactive mode: only use git when asked, stage changes and propose a commit message for user review. In autonomous/orchestrator mode (e.g. ralph wiggum): commit after each completed task with a descriptive message.
4. **Always** prior to finishing a task and considering it completed, revise all the changes and update Project Specification files.
5. When trying to fix any bug or error **ALWAYS** think carefully and analyze in detail what happened and WHY? Explain and confirm with user.


# Code Conventions

1. Project structure:
   - `app/` - FastAPI application code
   - `app/routers/` - API endpoint definitions
   - `app/services/` - Data access and business logic
   - `app/core/` - Shared utilities (auth, caching, logging, etc.)
   - `app/config/` - Resource configuration files
   - `tests/` - Two lanes, assigned automatically by fixture: `integration` (takes `server_url`, hits real data over HTTP against the in-process app or a `--server-url` deployment) and `offline` (no server, no network, no credentials)
   - `scripts/` - Utility scripts
   - `docs/` - Project documentation
2. Code should be self-descriptive
   - Only add comments for tricky or complex parts of the code (explaining WHY something is done)
   - NO redundant and trivial comments that simply restate what the code does
3. Private fields and methods should be prefixed with underscore
4. Use `ruff` for linting (`ruff check`)
5. Git commit messages should be concise and descriptive


# API Conventions

1. All routes are under `/api/v1` prefix
2. Data access uses tabix-indexed files on Google Cloud Storage via htslib
3. Configuration-driven: resources/datasets defined in `app/config/` modules
4. Authentication via JWT tokens with public endpoint decorator `@is_public`
5. Most tests are integration tests driven over HTTP; `pytest` boots the app in-process on a free port, `pytest --server-url <url>` targets a running deployment instead


# Running

1. Python 3.13 with `uv` for dependency management
2. `uv pip install -r pyproject.toml` for dependencies, `uv pip install -e ".[dev]"` for dev
3. Server: `python run_server.py [port]` (default port 4000)
4. Tests: `pytest` (boots the app in-process; needs GCS credentials), `pytest -m offline` (no network, no credentials — collection included, so no `app` module may reach the network *or* construct a client needing Application Default Credentials at import time; that is any Google client, not just GCS — `google.cloud.logging.Client()` was the second offender after `DatasetMapping`), `pytest --server-url http://host:port` (against a deployment)
5. Lint: `ruff check`
6. `app` is a **namespace package** and is not installed — `tests/conftest.py` puts the repo
   root on `sys.path`, and a bare `python scripts/…` needs `PYTHONPATH=<this checkout>`.
   Because namespace packages merge every matching directory on `sys.path`, a `PYTHONPATH`
   left pointing at another checkout silently adds that tree's `app/` to `app.__path__`.
   `pytest_configure` aborts the run when any `app.__path__` entry falls outside the pytest
   rootdir (genetics-results-suite-6o3, this repo's variant of it)


====

**Don't forget any of the 'QUALITY CODING RULES' above!!!**
