"""
Gene-to-disease configuration.

This module contains configuration for gene-to-disease relationship data
from multiple sources that are harmonized at load time.
"""

from app.config.profile import load_profile_module

_profile = load_profile_module("gene_disease")
gene_disease = _profile.gene_disease
