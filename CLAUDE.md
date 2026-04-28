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


# Software Development Behavior Guidelines

1. Don't guess and do things which you are not certain about. Ask the user instead.
2. Don't add or modify code unrelated to the specific request and context at the moment.
3. Only use git when asked, and when using git, only stage changes and propose a commit message. Let the user review the changes and commit them.
4. **Always** prior to finishing a task and considering it completed, revise all the changes and update Project Specification files.
5. When trying to fix any bug or error **ALWAYS** think carefully and analyze in detail what happened and WHY? Explain and confirm with user.


# Code Conventions

1. Project structure:
   - `app/` - FastAPI application code
   - `app/routers/` - API endpoint definitions
   - `app/services/` - Data access and business logic
   - `app/core/` - Shared utilities (auth, caching, logging, etc.)
   - `app/config/` - Resource configuration files
   - `tests/` - Integration tests (run against a live server)
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
5. Tests are integration tests run against a live server (`pytest --server-url <url>`)


# Running

1. Python 3.13 with `uv` for dependency management
2. `uv pip install -r requirements.txt` for dependencies, `uv pip install -e ".[dev]"` for dev
3. Server: `python run_server.py [port]` (default port 4000)
4. Tests: `pytest` or `pytest --server-url http://host:port`
5. Lint: `ruff check`


====

**Don't forget any of the 'QUALITY CODING RULES' above!!!**


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
