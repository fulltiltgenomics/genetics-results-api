"""FinnGen profile: gene-based results data paths."""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "dataset_id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "gcloud",
        "gencode_version": 35,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results4/genebass/gene_burden_results.tsv.gz",
        },
    },
    {
        "id": "schema_gene_based",
        "dataset_id": "schema_gene_based",
        "resource": "schema",
        "data_source": "gcloud",
        "gencode_version": 39,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results4/schema/SCHEMA2_gene_results.munged.tsv.gz",
        },
    },
    {
        "id": "bipex_gene_based",
        "dataset_id": "bipex_gene_based",
        "resource": "bipex",
        "data_source": "gcloud",
        "gencode_version": 39,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results4/bipex/BipEx2_gene_results.munged.tsv.gz",
        },
    },
    {
        "id": "ibd_ibd_gene_based",
        "dataset_id": "ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results4/ibd/IBD_exome_IBD_gene_results.munged.tsv.gz",
        },
    },
    {
        "id": "ibd_uc_gene_based",
        "dataset_id": "ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results4/ibd/IBD_exome_UC_gene_results.munged.tsv.gz",
        },
    },
    {
        "id": "ibd_cd_gene_based",
        "dataset_id": "ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results4/ibd/IBD_exome_CD_gene_results.munged.tsv.gz",
        },
    },
]
