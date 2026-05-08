"""Daly profile: gene-based results data paths."""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "dataset_id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "gcloud",
        "gencode_version": 35,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/genebass/gene_burden_results.tsv.gz",
        },
    },
    {
        "id": "schema_gene_based",
        "dataset_id": "schema_gene_based",
        "resource": "schema",
        "data_source": "gcloud",
        "gencode_version": 39,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/schema/SCHEMA2_gene_results.munged.noflag.tsv.gz",
        },
    },
    {
        "id": "bipex_gene_based",
        "dataset_id": "bipex_gene_based",
        "resource": "bipex",
        "data_source": "gcloud",
        "gencode_version": 39,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/bipex/BipEx2_gene_results.munged.mlog10p_gt4.noflag.tsv.gz",
        },
    },
    {
        "id": "ibd_ibd_gene_based",
        "dataset_id": "ibd_ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/ibd/IBD_exome_2026_IBD_gene_results.munged.noflag.tsv.gz",
        },
    },
    {
        "id": "ibd_uc_gene_based",
        "dataset_id": "ibd_uc_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/ibd/IBD_exome_2026_UC_gene_results.munged.noflag.tsv.gz",
        },
    },
    {
        "id": "ibd_cd_gene_based",
        "dataset_id": "ibd_cd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/ibd/IBD_exome_2026_CD_gene_results.munged.noflag.tsv.gz",
        },
    },
]
