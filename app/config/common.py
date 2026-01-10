"""
Common configuration settings used across the application.

This module contains general settings like authentication, database paths,
chunk sizes, and other common constants.
"""

import os

log_level = "INFO"
deploy_env = os.environ.get("DEPLOY_ENV", "dev1")

# use Cloud Logging API directly on VM
# on GKE, stdout is captured automatically so this should be False
use_cloud_logging_api = deploy_env.startswith("dev")

# usage logging (for BigQuery export via GCP log sink)
usage_logging_enabled = True
usage_logging_excluded_paths = {
    "/healthz",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/favicon.ico",
}

# CORS settings
cors_origins = [
    "https://anno.finngen.fi",
    "https://annopublic.finngen.fi",
]

# session settings
_session_secret_key = os.environ.get("SESSION_SECRET_KEY")
if not _session_secret_key:
    raise RuntimeError("SESSION_SECRET_KEY environment variable must be set")
session_secret_key = _session_secret_key
session_max_age = 24 * 60 * 60 * 365  # 1 year
session_https_only = True

# authentication
authentication = False
authentication_file = "/mnt/disks/data/finngen_auth_dev.json"

# data paths
hgnc_file = "/mnt/disks/data/hgnc_complete_set.txt"

metadata_db = "/mnt/disks/data/meta_finngen_version_20250526.db"

rsid_db = {
    "file": "/mnt/disks/data/gnomad/gnomad.genomes.exomes.v4.0.rsid.db",
}

gnomad = {
    "file": "/mnt/disks/data/gnomad/gnomad.genomes.exomes.v4.0.sites.v2.tsv.bgz",
    "populations": ["afr", "amr", "asj", "eas", "fin", "mid", "nfe", "oth", "sas"],
    "url": "https://gnomad.broadinstitute.org/variant/[VARIANT]?dataset=gnomad_r4",
    "version": "4.0",
}

dataset_to_resource = {
    "FinnGen_ATACseq": ("finngen", "R12"),
    "FinnGen_snRNAseq": ("finngen", "R12"),
    "FinnGen_Olink": ("finngen", "batch1_4"),
    "FinnGen_SomaScan": ("finngen", "2023-03-02"),
    "FinnGen_R13": ("finngen", "R13"),
    "FinnGen_R12": ("finngen", "R12"),
    "FinnGen_kanta": ("finngen", "R12"),
    "FinnGen_drugs": ("finngen", "R12"),
    "FinnGen_NMR": ("finngen_nmr", "1"),
    "FinnLiver": ("finnliver", "1"),
    "GeneRisk": ("generisk", "1"),
    "INTERVAL": ("interval", "1"),
    "UKB_PPP": ("ukbb", "3k"),
    "UKB_Finucane": ("ukbb", "1"),
    "Open_Targets_25.12": ("open_targets", "25.12"),
    "GTEx_v10": ("gtex", "v10"),
    "HPA_24.1": ("hpa", "24.1"),
    "genebass": ("genebass", "v1"),
}

dataset_mapping_files = [
    (
        "/mnt/disks/data/eqtl_catalogue_r7/dataset_metadata.tsv",
        "dataset_id",
        "eqtl_catalogue",
        "R7",
    ),
]

read_chunk_size = 16 * 1024  # 16KB
response_chunk_size = 64 * 1024  # 64KB
max_range_size_stream = 1e7  # 10Mb
max_range_size_json = 5e6  # 5Mb
max_gene_window = 3e6  # 3Mb
max_query_variants = 2000

variant_set_files = {
    "FinnGen_enriched_202505": {
        "file": "/mnt/disks/data/variant_sets/FinnGen_enriched_202505",
    },
    "COVID19_HGI_all": {
        "file": "/mnt/disks/data/variant_sets/COVID19_HGI_all",
    },
    "COVID19_HGI_severity": {
        "file": "/mnt/disks/data/variant_sets/COVID19_HGI_severity",
    },
}

phenotype_markdown_template = "gs://finngen-commons/results_api_data/phenotype_reports/{resource}/{phenocode}_gene_summary.md"

coding_set = set(
    [
        "missense_variant",
        "frameshift_variant",
        "inframe_insertion",
        "inframe_deletion",
        "transcript_ablation_variant",
        "stop_gained",
        "stop_lost",
        "start_lost",
        "splice_acceptor_variant",
        "splice_donor_variant",
        "incomplete_terminal_codon_variant",
        "protein_altering_variant",
        "coding_sequence_variant",
    ]
)
