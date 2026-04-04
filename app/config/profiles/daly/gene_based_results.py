"""Daly profile: gene-based results data paths."""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "gcloud",
        "gencode_version": 35,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/genebass/gene_burden_results.tsv.gz",
        },
        "metadata": {
            "metadata_file": "gs://daly-genetics-results/mapping_files/genebass_pheno_results.txt.bgz",
            "type": "genebass",
            "author": "GeneBass",
            "publication_date": "2022-09-14",
            "version_label": "N_394841",
        },
    },
]
