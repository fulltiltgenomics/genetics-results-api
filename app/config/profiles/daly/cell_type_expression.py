"""Daly profile: no default CELLxGENE release pointer.

The pointer is the on/off switch for the cell-type expression route, so a daly deployment opts
in by setting CELL_TYPE_EXPRESSION_RELEASE_POINTER (from the `cell_type_expression_release_pointer`
tfvar in genetics-results-suite) rather than inheriting one from the profile. Unset, the route
answers 404 "not configured" and the browser hides its section. `releases/CURRENT` is a small JSON
object written last by the release driver; its `uri` locates the release directory the server reads.
"""

release_pointer = None
refresh_seconds = 900
