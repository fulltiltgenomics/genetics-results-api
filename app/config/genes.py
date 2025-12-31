"""
Gene-related configuration including file paths and version information.
"""

genes = {
    "model_file": "/mnt/disks/data/ensembl_anno_coding_canonical_105.tsv.gz",
    "gencode_versions": [49, 43, 39, 35, 32],
    "gene_name_mapping_file": "gs://finngen-commons/results_api_data/mapping_files/gencode_gene_name_mapping_49-43-39-35-32.tsv",
    "gene_position_file_template": "gs://finngen-commons/results_api_data/mapping_files/gencode.v{version}.annotation.genes.tsv",
    "hgnc_file": "/mnt/disks/data/hgnc_complete_set.txt",
}
