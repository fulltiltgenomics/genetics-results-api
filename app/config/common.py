"""
Common configuration settings used across the application.

This module contains general settings like authentication, database paths,
chunk sizes, and other common constants.
"""

import os

from app.config.profile import load_profile_module

log_level = "INFO"
deploy_env = os.environ.get("DEPLOY_ENV", "dev1")

# use Cloud Logging API directly on VM
# on GKE, stdout is captured automatically so this should be False
use_cloud_logging_api = deploy_env.startswith("dev")

# usage logging (for BigQuery export via GCP log sink)
usage_logging_enabled = True
usage_logging_excluded_paths = {
    "/healthz",
    "/api/v1/docs",
    "/api/v1/redoc",
    "/api/v1/openapi.json",
    "/favicon.ico",
}

# when True, require X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy)
# set REQUIRE_AUTH=true in environments where IAP/oauth2-proxy is in front of the service
require_auth = os.environ.get("REQUIRE_AUTH", "false").lower() in ("1", "true", "yes")

# shared secret for internal service-to-service auth
internal_api_secret = os.environ.get("INTERNAL_API_SECRET", "")

# chat backend URL for user token validation (optional, empty = disabled)
chat_backend_url = os.environ.get("CHAT_BACKEND_URL", "")

# bearer token auth: allowed email domains and specific emails
allowed_email_domains = {
    d.strip() for d in os.environ.get("ALLOWED_EMAIL_DOMAINS", "finngen.fi").split(",") if d.strip()
}
allowed_emails = {
    e.strip() for e in os.environ.get("ALLOWED_EMAILS", "").split(",") if e.strip()
}

# data paths and profile-specific settings loaded from the active profile
_profile = load_profile_module("common")

hgnc_file = _profile.hgnc_file
rsid_db = _profile.rsid_db
gnomad = _profile.gnomad
dataset_to_resource = _profile.dataset_to_resource
dataset_mapping_files = _profile.dataset_mapping_files
variant_set_files = _profile.variant_set_files
phenotype_markdown_template = _profile.phenotype_markdown_template

# CORS settings: profile-specific origins + any extra from env var
cors_origins = _profile.cors_origins

read_chunk_size = 16 * 1024  # 16KB
response_chunk_size = 64 * 1024  # 64KB
max_range_size_stream = 1e7  # 10Mb
max_range_size_json = 5e6  # 5Mb
max_gene_window = 3e6  # 3Mb
max_query_variants = 2000

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
