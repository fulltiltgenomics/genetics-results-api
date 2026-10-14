"""
Single-cell (CELLxGENE) expression by cell type: configuration.

Two tabix files per release, both point-indexed on gene_start (`-s2 -b3 -e3`), in the
GENCODE v49 coordinates the genes are looked up in. Their column lists are the
`SUMMARY_COLUMNS` / `PER_DATASET_COLUMNS` constants of genetics-results-munge
scripts/cellxgene/publish.py; a release whose header differs is refused rather than
served under the wrong names.

The profile names only the release POINTER; the release directory it locates changes
under a running server (see app.services.data_access_cell_type_expression).
"""

from app.config.profile import load_profile_module

GENCODE_VERSION = 49

SUMMARY_FILE = "summary.tsv.gz"
PER_DATASET_FILE = "per_dataset.tsv.gz"
MEASURED_GENES_FILE = "measured_genes.tsv.gz"
DATASETS_FILE = "bq/datasets.tsv"
GROUPS_FILE = "bq/groups.tsv.gz"
MANIFEST_FILE = "manifest.json"

_GENE_COLUMNS = ["gene_id", "chrom", "gene_start", "gene_end", "gene_name"]

summary_columns = _GENE_COLUMNS + [
    "source_gene_ids", "cell_type_rollup_ontology_term_id", "cell_type_rollup",
    "tissue_general_ontology_term_id", "tissue_general", "n_cells", "n_donors", "n_datasets",
    "mean_log1p_cp10k", "frac_expressing", "donor_pseudobulk_mean_cp10k",
    "donor_pseudobulk_n_donors", "tau",
]

per_dataset_columns = _GENE_COLUMNS + ["source_gene_id"] + [
    "dataset_id", "tissue_ontology_term_id", "tissue", "tissue_type",
    "tissue_general_ontology_term_id", "tissue_general", "cell_type_ontology_term_id", "cell_type",
    "cell_type_rollup_ontology_term_id", "cell_type_rollup", "disease_ontology_term_id", "disease",
    "assay_ontology_term_id", "assay", "suspension_type", "spatial", "n_cells", "n_donors",
    "mean_log1p_cp10k", "frac_expressing", "donor_pseudobulk_mean_cp10k",
]

# appended to the per-dataset level by this API; not a column of the file
STATUS_COLUMN = "status"
STATUS_PRESENT = "present"
STATUS_MEASURED_NO_ROW = "measured_no_row"

_types = {
    "gene_id": str, "chrom": int, "gene_start": int, "gene_end": int, "gene_name": str,
    "source_gene_ids": str, "source_gene_id": str, "dataset_id": str,
    "n_cells": int, "n_donors": int, "n_datasets": int, "donor_pseudobulk_n_donors": int,
    "mean_log1p_cp10k": float, "frac_expressing": float,
    "donor_pseudobulk_mean_cp10k": float, "tau": float, "spatial": bool,
}


def _schema(columns: list[str]) -> dict[str, type]:
    return {c: _types.get(c, str) for c in columns}

summary_header_schema = _schema(summary_columns)
per_dataset_header_schema = _schema(per_dataset_columns + [STATUS_COLUMN])

_profile = load_profile_module("cell_type_expression")
# gs:// URI of the release pointer object; None where the deployment has no such data
release_pointer: str | None = _profile.release_pointer
refresh_seconds: int = _profile.refresh_seconds
