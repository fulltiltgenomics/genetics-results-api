#!/usr/bin/env sh
# Warns when a commit changes something the docs describe but leaves the doc
# untouched. Mappings mirror the "Documentation ownership" table in CLAUDE.md.
#
# This never blocks. A warning that is occasionally ignored beats a gate that
# gets bypassed with --no-verify, because a bypassed gate is both absent and
# assumed present.

set -u

root=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
cd "$root" || exit 0

staged=$(git diff --cached --name-only --diff-filter=ACMRD)
[ -n "$staged" ] || exit 0

hit() {
    printf '%s\n' "$staged" | grep -qE "$1"
}

found=0
check() {
    if hit "$1" && ! hit "$2"; then
        if [ "$found" -eq 0 ]; then
            printf '\ndoc-drift warning — this commit changes code the docs describe:\n\n' >&2
            found=1
        fi
        printf '  %s\n' "$3" >&2
    fi
}

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

if [ "$found" -eq 1 ]; then
    printf '\n  Update the doc in this commit, or note why it does not apply.\n' >&2
    printf '  Not blocking. Mappings live in CLAUDE.md > Documentation ownership.\n\n' >&2
fi

exit 0
