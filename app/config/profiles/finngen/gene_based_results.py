"""FinnGen profile: gene-based results data paths.

`file` is the gene-locus-indexed file that /gene_based/{gene} queries across all
traits of the dataset. For genebass that file holds the mlog10p_burden > 4 hits
only — the unfiltered results are 343M rows, too many to sort into one tabixed
file. `prefix`/`suffix` point at the per-trait copies, which are unfiltered for
every dataset and back /gene_based_results_by_phenotype.
"""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "dataset_id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "gcloud",
        "example_pheno_or_study": "categorical_41210_both_sexes_S068_",
        "gencode_version": 35,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/genebass/gene_burden_results.mlog10p_gt4.tsv.gz",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/genebass/gene_burden_per_trait/",
            "suffix": ".tsv.gz",
        },
    },
    {
        "id": "schema_gene_based",
        "dataset_id": "schema_gene_based",
        "resource": "schema",
        "data_source": "gcloud",
        "example_pheno_or_study": "schizophrenia",
        "gencode_version": 39,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/schema/SCHEMA2_gene_results.munged.tsv.gz",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/schema/gene_burden_per_trait/",
            "suffix": ".tsv.gz",
        },
    },
    {
        "id": "bipex_gene_based",
        "dataset_id": "bipex_gene_based",
        "resource": "bipex",
        "data_source": "gcloud",
        "example_pheno_or_study": "bipolar_disorder",
        "gencode_version": 39,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/bipex/BipEx2_gene_results.munged.tsv.gz",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/bipex/gene_burden_per_trait/",
            "suffix": ".tsv.gz",
        },
    },
    {
        "id": "ibd_ibd_gene_based",
        "dataset_id": "ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "example_pheno_or_study": "inflammatory_bowel_disease",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/ibd/IBD_exome_IBD_gene_results.munged.tsv.gz",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/ibd/gene_burden_per_trait/",
            "suffix": ".tsv.gz",
        },
    },
    {
        "id": "ibd_uc_gene_based",
        "dataset_id": "ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "example_pheno_or_study": "ulcerative_colitis",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/ibd/IBD_exome_UC_gene_results.munged.tsv.gz",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/ibd/gene_burden_per_trait/",
            "suffix": ".tsv.gz",
        },
    },
    {
        "id": "ibd_cd_gene_based",
        "dataset_id": "ibd_gene_based",
        "resource": "ibd",
        "data_source": "gcloud",
        "example_pheno_or_study": "crohns_disease",
        "gencode_version": 43,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/ibd/IBD_exome_CD_gene_results.munged.tsv.gz",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/ibd/gene_burden_per_trait/",
            "suffix": ".tsv.gz",
        },
    },
]
