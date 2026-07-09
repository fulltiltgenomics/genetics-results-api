"""
Open-chromatin (Product A) data configuration including schemas and data sources.

This module contains all configuration related to the open_chromatin data product:
- Header schema for the canonical open_chromatin TSV (an atlas of accessible /
  active regulatory regions labeled by cell type / tissue / condition)
- Per-profile open_chromatin data source configurations

Query model differs from chromatin_peaks: open_chromatin supports genomic-range and
variant-position OVERLAP queries (a peak matches when its [start, end] interval
overlaps the queried region/position), served by tabix range reads.
"""

from app.config.profile import load_profile_module

# canonical open_chromatin schema. The columns after "resource" are the exact TSV
# column order produced by the munge step; "resource" is prepended by the data
# access layer (each file is per-resource, and the file already carries its own
# "version" column, so only "resource" is added).
open_chromatin_header_schema = {
    "resource": str,
    "chrom": str,  # numeric string "1".."22","23"(X),"24"(Y),"25"(M/MT), no "chr" prefix
    "start": int,
    "end": int,
    "peak_id": str,
    "dataset": str,
    "cell_type": str,
    "tissue": str,
    "life_stage": str,
    "condition": str,
    "assay": str,
    "score": float,
    "score_type": str,
    "n_cells": int,
    "cell_ontology_id": str,
    "uberon_id": str,
    "target_gene": str,
    "target_gene_id": str,
    "version": str,
}

_profile = load_profile_module("open_chromatin")
open_chromatin_data = _profile.open_chromatin_data
