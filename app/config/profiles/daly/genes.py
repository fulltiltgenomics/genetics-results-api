"""Daly profile: gene data paths."""

genes = {
    # ensembl annotation file not available in this bucket; fall back to finngen-commons
    "model_file": "gs://finngen-commons/results_api_data/mapping_files/ensembl_anno_coding_canonical_105.tsv.gz",
    "gencode_versions": [49, 43, 39, 35, 32, 19],
    "gene_name_mapping_file": "gs://daly-genetics-results/mapping_files/gencode_gene_name_mapping_49-43-39-35-32-19.tsv",
    "gene_position_file_template": "gs://daly-genetics-results/mapping_files/gencode.v{version}.annotation.genes.tsv",
    "hgnc_file": "gs://daly-genetics-results/mapping_files/hgnc_complete_set.txt",
}
