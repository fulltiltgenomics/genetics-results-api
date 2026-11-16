#!/usr/bin/env sh
# Warns when a commit changes something the docs describe but leaves the doc
# untouched. Mappings mirror the "Documentation ownership" table in CLAUDE.md.
#
# The rules are this repo's own; `check` and the rest of the mechanism live in
# scripts/doc-drift-lib.sh, which is the same file in every repo of the suite.

. "$(dirname "$0")/doc-drift-lib.sh"

DOC_SPEC='^docs/project-spec\.md$'

check '^app/routers/' "$DOC_SPEC" \
    'app/routers/ -> docs/project-spec.md (the API Endpoints table — one row per router registered in app/server.py)'

check '^app/config/' "$DOC_SPEC" \
    'app/config/ -> docs/project-spec.md (per-product config, which datasets/resources/files exist, profile config keys)'

check '^app/core/auth\.py$' "$DOC_SPEC" \
    'app/core/auth.py -> docs/project-spec.md (the Authentication section: accepted credential order, env vars, @is_public list)'

check '^app/services/startup_checks\.py$' "$DOC_SPEC" \
    'app/services/startup_checks.py -> docs/project-spec.md (the enumerated data-file families, what is excluded, the file counts)'

check '^pyproject\.toml$' "$DOC_SPEC" \
    'pyproject.toml -> docs/project-spec.md (Tech Stack: Python version, dependency claims, why PyJWT stays pinned)'

check '^scripts/(lint-staged|install-git-hooks)\.sh$' '^(docs/project-spec\.md|README\.md)$' \
    'lint gate (scripts/lint-staged.sh, install-git-hooks.sh) -> README.md + docs/project-spec.md (which commits it blocks, the ruff rule set and pinned version, how ruff is resolved in a worktree)'

doc_drift_finish
