"""
Gene-related configuration including file paths and version information.
"""

from app.config.profile import load_profile_module

_profile = load_profile_module("genes")
genes = _profile.genes
