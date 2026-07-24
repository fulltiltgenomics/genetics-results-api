"""
MPRA (massively parallel reporter assay) data configuration including schemas and
data sources.

This module contains all configuration related to the mpra data product:
- Header schema for the canonical mpra TSV (Siraj et al. 2026 measured intrinsic
  cis-regulatory allelic activity: emVar / active / log2Skew / log2FC calls)
- Per-profile mpra data source configurations

Query model: the file is VARIANT/POINT-indexed (tabix -s1 -b2 -e2, begin == end ==
pos). A variant query is a point read at pos (optionally filtered to a ref/alt), a
region query is a positional range read, and a by-gene query resolves the gene to a
region and range-reads it. Served by the same GCS tabix engine as variant_effect.

Config granularity: one entry per DATASET. The Siraj resource ships a single LONG
tabix file (one row per variant x cell_line, cell_line in {meta,K562,HEPG2,SKNSH,
HCT116,A549}), so there is one per-dataset entry keyed by ``dataset_id`` carrying the
prepended ``resource`` ("siraj_mpra").
"""

from app.config.profile import load_profile_module

# canonical mpra schema. The columns after "resource" are the exact TSV column order
# produced by the munge step (LONG: one row per variant x cell_line); "resource" is
# prepended by the data access layer. emVar / active are call flags; log2Skew_se is
# nullable (NA for per-cell-line rows, populated only for the meta row) and the
# per-row *_mlog10p values may be NA, handled by the TSV -> typed parser.
mpra_header_schema = {
    "resource": str,
    "chrom": str,  # numeric string "1".."22","23"(X),"24"(Y),"25"(M/MT), no "chr" prefix
    "pos": int,
    "variant": str,
    "ref": str,
    "alt": str,
    "cohort": str,
    "cell_line": str,  # meta | K562 | HEPG2 | SKNSH | HCT116 | A549
    "emVar": bool,
    "active": bool,
    "log2Skew": float,
    "log2Skew_se": float,
    "log2Skew_mlog10p": float,
    "log2FC": float,
    "log2FC_mlog10p": float,
    "mean_RNA_ref": float,
    "mean_RNA_alt": float,
}

_profile = load_profile_module("mpra")
mpra_data = _profile.mpra_data
