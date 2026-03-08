"""Configuration for exome results data."""

# header schema for exome results
exome_header_schema = {
    "resource": str,  # added by API
    "version": str,  # added by API
    "dataset": str,
    "chr": int,
    "pos": int,
    "ref": str,
    "alt": str,
    "gene": str,
    "annotation": str,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "af_overall": float,
    "af_cases": float,
    "af_controls": float,
    "ac": int,
    "an": int,
    "heritability": float,
    "trait": str,
}

# column names for merging data across files
variant_columns = {
    "chr": b"chr",
    "pos": b"pos",
    "ref": b"ref",
    "alt": b"alt",
    "dataset": b"dataset",
}

# exome data files configuration
exome_data_files = [
    {
        "id": "genebass",
        "resource": "genebass",
        "data_source": "gcloud",
        "example_pheno_or_study": "categorical_41210_both_sexes_S068_",
        "gencode_version": 35,
        "metadata": {
            "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/genebass_pheno_results.txt.bgz",
            "type": "genebass",
            "author": "GeneBass",
            "publication_date": "2022-01-01",
            "version_label": "500k",
        },
        "exome": {
            "version": "v1",
            "prefix": "gs://finngen-commons/results_api_data/exome_results/genebass/individual/",
            "suffix": ".mlog10p4.tsv",
            "all_exome_file": "gs://finngen-commons/results_api_data/exome_results/genebass/genebass_variant_results_mlog10p4.tsv.gz",
        },
    },
]

# build lookup dictionaries
exome_data_file_by_id = {df["id"]: df for df in exome_data_files}

# build resource to data file IDs mapping
resource_to_exome_data_file_ids = {}
for df in exome_data_files:
    resource = df["resource"]
    if resource not in resource_to_exome_data_file_ids:
        resource_to_exome_data_file_ids[resource] = []
    resource_to_exome_data_file_ids[resource].append(df["id"])
