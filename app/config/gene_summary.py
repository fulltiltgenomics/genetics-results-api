"""
AI gene summaries: configuration.

A release is a bucket folder holding one `<SYMBOL>.json` per summarized gene, keyed by HGNC
approved symbol, and a `manifest.json`. genetics-results-workflows gene_summary/publish.py
writes both. One object per gene, so a refresh uploads only the genes whose evidence changed
and a running server serves them without a restart.
"""

import os

from app.config.profile import load_profile_module

MANIFEST_FILE = "manifest.json"

_profile = load_profile_module("gene_summary")
# gs:// folder of the release, ending in "/"; None where the deployment has no summaries. The
# env override lets a second cluster of the same profile (staging) read its own release; empty
# counts as unset.
prefix: str | None = os.environ.get("GENE_SUMMARY_PREFIX") or _profile.prefix

if prefix is not None and not prefix.endswith("/"):
    raise ValueError(f"gene summary prefix must end with '/': {prefix!r}")
