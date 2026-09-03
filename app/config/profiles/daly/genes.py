"""Daly profile: gene data paths."""

genes = {
    "gencode_versions": [49, 45, 43, 39, 35, 32, 19],
    "gene_name_mapping_file": "gs://daly-genetics-results/mapping_files/gencode_gene_name_mapping_49-45-43-39-35-32-19.tsv",
    "gene_position_file_template": "gs://daly-genetics-results/mapping_files/gencode.v{version}.annotation.genes.tsv",
    # exon structure, per GENCODE version that has it. A version with no entry serves empty
    # exon arrays rather than borrowing another version's: the gene ids carry a version
    # suffix and the coordinates move between releases, so exons from one release attached
    # to another's gene bodies would be silently misplaced. Only the newest release is
    # published today because exons are a drawing surface and drawing is always done at the
    # newest, while the older versions exist to match each dataset's declared
    # `gencode_version` for gene-coordinate lookup.
    #
    # One row per exon of each gene's Ensembl-canonical transcript, so every gene in
    # gencode.vNN.annotation.genes.tsv has exactly one transcript here. Built by
    # genetics-results-munge scripts/gencode_to_exon_tsv_gff.py.
    "exon_file_by_version": {
        49: "gs://daly-genetics-results/mapping_files/gencode.v49.annotation.exons.canonical.tsv.gz",
    },
    "hgnc_file": "gs://daly-genetics-results/mapping_files/hgnc_complete_set.txt",
    "gene_has_family_file": "gs://daly-genetics-results/mapping_files/hgnc_gene_has_family.csv",
    "hierarchy_closure_file": "gs://daly-genetics-results/mapping_files/hgnc_hierarchy_closure.csv",
    "family_file": "gs://daly-genetics-results/mapping_files/hgnc_family.csv",
}
