"""FinnGen profile: colocalization data paths."""

coloc = [
    {
        "name": "FinnGen_R13_vs_many",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/colocQC.munged.tsv.gz",
    },
    {
        "name": "FinnGen_eQTL_vs_R12",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.eQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.eQTL.colocQC.munged.tsv.gz",
    },
    {
        "name": "FinnGen_eQTL_vs_KANTA",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.eQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.eQTL.colocQC.munged.tsv.gz",
    },
    {
        "name": "FinnGen_caQTL_vs_R12",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.caQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-R12.caQTL.colocQC.munged.tsv.gz",
    },
    {
        "name": "FinnGen_caQTL_vs_KANTA",
        "data_source": "gcloud",
        "credset_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.caQTL.coloc.credsets.munged.tsv.gz",
        "coloc_file": "gs://finngen-commons/results_api_data/coloc/FinnGen-KANTA.caQTL.colocQC.munged.tsv.gz",
    },
]
