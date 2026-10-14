"""Daly profile: the CELLxGENE cell-type expression release pointer.

`releases/CURRENT` is a small JSON object written last by the release driver; its `uri`
locates the release directory the server reads.
"""

release_pointer = "gs://daly-expression-data/cellxgene/releases/CURRENT"
refresh_seconds = 900
