"""FinnGen profile: gene data paths."""

genes = {
    "model_file": "gs://finngen-commons/results_api_data/mapping_files/ensembl_anno_coding_canonical_105.tsv.gz",
    "gencode_versions": [49, 43, 39, 35, 32, 19],
    "gene_name_mapping_file": "gs://finngen-commons/results_api_data/mapping_files/gencode_gene_name_mapping_49-43-39-35-32-19.tsv",
    "gene_position_file_template": "gs://finngen-commons/results_api_data/mapping_files/gencode.v{version}.annotation.genes.tsv",
    "hgnc_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_complete_set.txt",
    "gene_has_family_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_gene_has_family.csv",
    "hierarchy_closure_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_hierarchy_closure.csv",
    "family_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_family.csv",
}
