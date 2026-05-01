"""
Expression data configuration including schemas and data sources.

This module contains all configuration related to expression data:
- Header schema for expression data validation
- Expression data source configurations (GTEx, HPA, etc.)
"""

from app.config.profile import load_profile_module

expression_header_schema = {
    "resource": str,
    "version": str,
    "dataset": str,
    "chrom": int,
    "gene_start": int,
    "gene_end": int,
    "gene_name": str,
    "gene_id": str,
    "tissue_cell": str,
    "level": str,  # can be numeric or string depending on data source
}

# column names for merging data across files
simple_columns = {
    "dataset": b"dataset",
}

_profile = load_profile_module("expression")
expression_data = _profile.expression_data
