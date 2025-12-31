"""
Credible sets configuration including schemas and data files.
"""

cs_header_schema = {
    "resource": str,  # this is added to the header by the api
    "version": str,  # this is added to the header by the api
    "dataset": str,
    "data_type": str,
    "trait": str,
    "trait_original": str,
    "cell_type": str,
    "chr": int,
    "pos": int,
    "ref": str,
    "alt": str,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "pip": float,
    "cs_id": str,
    "cs_size": int,
    "cs_min_r2": float,
    "aaf": float,
    "most_severe": str,
    "gene_most_severe": str,
}

cs_qtl_header_schema = {
    "resource": str,  # this is added to the header by the api
    "version": str,  # this is added to the header by the api
    "dataset": str,
    "data_type": str,
    "trait": str,
    "trait_original": str,
    "cell_type": str,
    "chr": int,
    "pos": int,
    "ref": str,
    "alt": str,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "pip": float,
    "cs_id": str,
    "cs_size": int,
    "cs_min_r2": float,
    "aaf": float,
    "most_severe": str,
    "gene_most_severe": str,
    "trait_chr": int,
    "trait_start": int,
    "trait_end": int,
}

# column names for merging data across files
variant_columns = {
    "chr": b"chr",
    "pos": b"pos",
    "ref": b"ref",
    "alt": b"alt",
    "dataset": b"dataset",
}

qtl_columns = {
    "trait_start": b"trait_start",
    "trait_end": b"trait_end",
    "dataset": b"dataset",
}

data_files = [
    {
        "id": "finngen_gwas",
        "resource": "finngen",
        "data_source": "gcloud",
        "example_pheno_or_study": "AUTOIMMUNE",
        "gencode_version": 49,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/finngen_core/r13_20251024/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_core/r13_20251024/FinnGen_R13_credible_sets.tsv.gz",
        },
        "metadata": {
            "metadata_file": "/mnt/disks/data/finngen_r13_pheno_202509.json",
            "type": "finngen_r13",
            "author": "FinnGen Consortium",
            "publication_date": "2025-09-01",
            "version_label": "R13",
        },
    },
    {
        "id": "finngen_kanta",
        "resource": "finngen",
        "data_source": "gcloud",
        "example_pheno_or_study": "3001122",
        "gencode_version": 49,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/finngen_kanta/r12_20251024/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_kanta/r12_20251024/FinnGen_R12kanta_credible_sets.tsv.gz",
        },
        "metadata": {
            "metadata_file": "/mnt/disks/data/Kanta_metadata_202503.tsv",
            "type": "finngen_kanta",
            "author": "FinnGen Consortium",
            "publication_date": "2025-03-01",
            "version_label": "kanta",
        },
    },
    {
        "id": "finngen_drugs",
        "resource": "finngen",
        "data_source": "gcloud",
        "example_pheno_or_study": "ATC_J01_IRN",
        "gencode_version": 49,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/finngen_drugs/r12_20251024/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_drugs/r12_20251024/FinnGen_R12drugs_credible_sets.tsv.gz",
        },
        "metadata": {
            "metadata_file": "/mnt/disks/data/drugs_pheno.json",
            "type": "finngen_drugs",
            "author": "FinnGen Consortium",
            "publication_date": "2024-01-01",
            "version_label": "R12",
        },
    },
    {
        "id": "finngen_pqtl",
        "resource": "finngen",
        "data_source": "gcloud",
        "example_pheno_or_study": "PCSK9",
        "gencode_version": 49,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/finngen_olink/20251024/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_olink/20251024/FinnGen_Olink_1-4_credible_sets.tsv.gz",
            "all_cs_qtl_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_olink/20251024/FinnGen_Olink_1-4_credible_sets.qtl.tsv.gz",
        },
    },
    {
        "id": "ukbb_pqtl",
        "resource": "ukbb_pqtl",
        "data_source": "gcloud",
        "example_pheno_or_study": "IL5RA",
        "gencode_version": 49,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/ukb_ppp/20251024/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/ukb_ppp/20251024/UKB_PPP_credible_sets.tsv.gz",
            "all_cs_qtl_file": "gs://finngen-commons/results_api_data/credible_sets/ukb_ppp/20251024/UKB_PPP_credible_sets.qtl.tsv.gz",
        },
    },
    {
        "id": "finngen_eqtl",
        "resource": "finngen",
        "data_source": "gcloud",
        "example_pheno_or_study": "predicted.celltype.l1.CD4_T.chr13",
        "gencode_version": 32,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/finngen_snrnaseq/20251024/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_snrnaseq/20251024/FinnGen_snRNAseq_202509_credible_sets.tsv.gz",
            "all_cs_qtl_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_snrnaseq/20251024/FinnGen_snRNAseq_202509_credible_sets.qtl.tsv.gz",
        },
    },
    {
        "id": "finngen_caqtl",
        "resource": "finngen",
        "data_source": "gcloud",
        "example_pheno_or_study": "predicted.celltype.l1.B.chrX",
        "gencode_version": 32,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/finngen_atacseq/20251118/glob-5c4b82b4bcd4288199af76c4cdfd7763/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/finngen_atacseq/20251118/FinnGen_ATACseq_202509_credible_sets.tsv.gz",
        },
    },
    {
        "id": "eqtl_catalogue",
        "resource": "eqtl_catalogue",
        "data_source": "gcloud",
        "example_pheno_or_study": "QTD000605",
        "gencode_version": 39,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/eqtl_catalogue/r7/individual/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/eqtl_catalogue/r7/eQTL_Catalogue_R7.tsv.gz",
            "all_cs_qtl_file": "gs://finngen-commons/results_api_data/credible_sets/eqtl_catalogue/r7/eQTL_Catalogue_R7.qtl.tsv.gz",
        },
        "metadata": {
            "metadata_file": "/mnt/disks/data/eqtl_catalogue_r7/dataset_metadata.tsv",
            "type": "eqtl_catalogue",
            "publication_date": "2020-01-01",
            "version_label": "R7",
        },
    },
    {
        "id": "open_targets",
        "resource": "open_targets",
        "data_source": "gcloud",
        "example_pheno_or_study": "GCST004602",
        "gencode_version": 49,
        "cs": {
            "prefix": "gs://finngen-commons/results_api_data/credible_sets/open_targets/202512/individual/",
            "suffix_95": ".SUSIE.munged.tsv",
            "all_cs_file": "gs://finngen-commons/results_api_data/credible_sets/open_targets/202512/Open_Targets_25.12_credible_sets.tsv.gz",
        },
        "metadata": {
            "metadata_file": "gs://finngen-commons/results_api_data/credible_sets/open_targets/202512/ot_2512_data_studies.json",
            "type": "open_targets",
            "publication_date": "2025-12-10",
            "version_label": "25.12",
        },
    },
]


def _build_data_file_by_id():
    """Build lookup dict from data file ID to data file config."""
    return {df["id"]: df for df in data_files}


def _build_resource_to_data_file_ids():
    """Build mapping from resource name to list of data file IDs."""
    mapping = {}
    for df in data_files:
        resource = df.get("resource", df["id"])
        if resource not in mapping:
            mapping[resource] = []
        mapping[resource].append(df["id"])
    return mapping


data_file_by_id = _build_data_file_by_id()
resource_to_data_file_ids = _build_resource_to_data_file_ids()
