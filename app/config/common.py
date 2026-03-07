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
    "https://finngenie.finngen.fi",
    "https://finngenie.fi",
]

# when True, require X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy)
# set REQUIRE_AUTH=true in environments where IAP/oauth2-proxy is in front of the service
require_auth = os.environ.get("REQUIRE_AUTH", "").lower() in ("1", "true", "yes")

# shared secret for internal service-to-service auth
internal_api_secret = os.environ.get("INTERNAL_API_SECRET", "")

# bearer token auth: allowed email domains and specific emails
allowed_email_domains = {
    d.strip() for d in os.environ.get("ALLOWED_EMAIL_DOMAINS", "finngen.fi").split(",") if d.strip()
}
allowed_emails = {
    e.strip() for e in os.environ.get("ALLOWED_EMAILS", "").split(",") if e.strip()
}

# data paths
hgnc_file = "gs://finngen-commons/results_api_data/mapping_files/hgnc_complete_set.txt"

rsid_db = {
    "file": "gs://finngen-commons/results_api_data/gnomad/gnomad.genomes.exomes.v4.0.rsid.tsv.gz",
}

gnomad = {
    "file": "gs://finngen-commons/results_api_data/gnomad/gnomad.genomes.exomes.v4.0.sites.v2.tsv.bgz",
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
        "gs://finngen-commons/results_api_data/mapping_files/eqtl_catalogue_r7_dataset_metadata.tsv",
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
        "file": "gs://finngen-commons/results_api_data/variant_sets/FinnGen_enriched_202505",
    },
    "COVID19_HGI_all": {
        "file": "gs://finngen-commons/results_api_data/variant_sets/COVID19_HGI_all",
    },
    "COVID19_HGI_severity": {
        "file": "gs://finngen-commons/results_api_data/variant_sets/COVID19_HGI_severity",
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
