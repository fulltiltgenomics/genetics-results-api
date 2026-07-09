"""
Variant-effect (Product B) data configuration including schemas and data sources.

This module contains all configuration related to the variant_effect data product:
- Header schema for the canonical variant_effect TSV (in-silico predicted variant
  effect on chromatin accessibility: ChromBPNet + FLARE)
- Per-profile variant_effect data source configurations

Query model: the files are VARIANT/POINT-indexed (tabix -s1 -b2 -e2, begin == end ==
pos). A variant query is a point read at pos (optionally filtered to a ref/alt), a
region query is a positional range read, and a by-gene query resolves the gene to a
region and range-reads it. Served by the same GCS tabix engine as open_chromatin.

Config granularity: one entry per DATASET (not per resource). The Marderstein resource
ships two distinct predictor files — marderstein_chrombpnet and marderstein_flare — so
each is a separate per-dataset entry keyed by ``dataset_id``. Both carry the same
prepended ``resource`` ("marderstein").
"""

from app.config.profile import load_profile_module

# canonical variant_effect schema. The columns after "resource" are the exact TSV
# column order produced by the munge step; "resource" is prepended by the data access
# layer. The file already carries "variant" and "version" columns, so only "resource"
# is added. mlog10p / quantile_rank / is_significant and the free-string context columns
# are nullable (NA in the file), handled by the TSV -> typed parser.
variant_effect_header_schema = {
    "resource": str,
    "chrom": str,  # numeric string "1".."22","23"(X),"24"(Y),"25"(M/MT), no "chr" prefix
    "pos": int,
    "ref": str,
    "alt": str,
    "variant": str,
    "rsid": str,
    "dataset": str,
    "model": str,
    "cell_type": str,
    "tissue": str,
    "life_stage": str,
    "score": float,
    "score_type": str,
    "mlog10p": float,
    "predicted_direction": str,
    "quantile_rank": float,
    "is_significant": bool,
    "version": str,
}

_profile = load_profile_module("variant_effect")
variant_effect_data = _profile.variant_effect_data
