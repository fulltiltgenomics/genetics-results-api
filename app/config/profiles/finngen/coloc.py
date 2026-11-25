"""FinnGen profile: colocalization data paths.

Each entry's `pairs` field lists (dataset_id, dataset_id) tuples describing
which datasets were colocalized. Both dataset_ids must exist in datasets.py.
A dataset appearing on both sides of a pair means self-colocalization.

coloc_gene_file is the copy of coloc_file indexed on the QTL gene instead of the
colocalizing region (genetics-results-munge scripts/coloc/create_gene_indexed_coloc_file.sh),
read by /colocalization_by_gene. gene_gencode_version is the GENCODE version its gene
coordinates came from: the route keeps the rows whose coordinates equal the query gene's,
so the two must be the same file's.
"""

coloc = [
    {
        "name": "FinnGen_R14_vs_many",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/finngen_r14_coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/finngen_r14_colocQC.munged.tsv.gz",
        "coloc_gene_file": "gs://finngen-commons/results_api_data/coloc/finngen_r14_colocQC.munged.gene.tsv.gz",
        "gene_gencode_version": 49,
        "pairs": [
            ("finngen_gwas", "finngen_gwas"),
            ("finngen_gwas", "finngen_kanta"),
            ("finngen_gwas", "finngen_pqtl"),
            ("finngen_gwas", "finngen_pqtl_5k"),
            ("finngen_gwas", "finngen_nmr"),
            ("finngen_gwas", "finngen_somascan"),
            ("finngen_gwas", "finnliver"),
            ("finngen_gwas", "generisk"),
            ("finngen_gwas", "interval"),
            ("finngen_gwas", "ukbb_finucane"),
            ("finngen_gwas", "ukbb_pqtl"),
            ("finngen_gwas", "eqtl_catalogue"),
            ("finngen_gwas", "finngen_eqtl"),
        ],
    },
    {
        "name": "FinnGen_eQTL_vs_R12",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.eQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.eQTL.colocQC.munged.tsv.gz",
        "coloc_gene_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.eQTL.colocQC.munged.gene.tsv.gz",
        "gene_gencode_version": 49,
        "pairs": [
            ("finngen_eqtl", "finngen_gwas_r12"),
        ],
    },
    {
        "name": "FinnGen_eQTL_vs_KANTA",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.eQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.eQTL.colocQC.munged.tsv.gz",
        "coloc_gene_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.eQTL.colocQC.munged.gene.tsv.gz",
        "gene_gencode_version": 49,
        "pairs": [
            ("finngen_eqtl", "finngen_kanta_r12"),
        ],
    },
    {
        "name": "FinnGen_caQTL_vs_R12",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.caQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.caQTL.colocQC.munged.tsv.gz",
        "pairs": [
            ("finngen_caqtl", "finngen_gwas_r12"),
        ],
    },
    {
        "name": "FinnGen_caQTL_vs_KANTA",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.caQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.caQTL.colocQC.munged.tsv.gz",
        "pairs": [
            ("finngen_caqtl", "finngen_kanta_r12"),
        ],
    },
]
