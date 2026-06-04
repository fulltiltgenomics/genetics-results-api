# Genetics Results API - Project Specification

## Purpose

A FastAPI-based REST API that serves human genetics association results and annotations. It provides read-only access to tabix-indexed data files stored on Google Cloud Storage, enabling queries by genomic region, variant, gene, and phenotype across multiple genetic studies and datasets.

## Architecture

### Tech Stack
- **Python 3.13** with **FastAPI** and **uvicorn**
- **htslib/tabix** for indexed genomic data access (compiled in Docker image)
- **Google Cloud Storage** for data files (accessed via gcloud-aio-storage, gcsfs)
- **Polars** for data manipulation
- **JWT** authentication (PyJWT)
- **uv** for dependency management
- Deployed as a Docker container (base: nikolaik/python-nodejs)

### Project Structure

```
app/
├── config/         # resource/dataset configuration modules
├── core/           # shared utilities
│   ├── auth.py         # JWT authentication
│   ├── cache.py        # caching layer
│   ├── datatypes.py    # common data types
│   ├── exceptions.py   # custom exceptions
│   ├── file_utils.py   # file handling utilities
│   ├── logging_config.py
│   ├── responses.py    # response formatting
│   ├── service_container.py  # dependency injection container
│   ├── streams.py      # streaming utilities
│   └── variant.py      # variant parsing/formatting
├── routers/        # API endpoint definitions (one per domain)
├── services/       # data access and business logic
├── dependencies.py # FastAPI dependency injection
├── middleware.py    # CORS and other middleware setup
├── middleware_usage_logging.py
└── server.py       # FastAPI app creation and router registration
tests/              # integration tests (against live server)
scripts/            # utility scripts
docs/               # project documentation
Dockerfile          # container build with htslib, gcloud SDK
run_server.py       # server entry point
```

### API Endpoints (all under `/api/v1`)

| Tag | Domain |
|-----|--------|
| authentication | Login/token management |
| metadata | Dataset and resource metadata |
| search | Search across phenotypes, genes, variants (includes gene coordinates via GENCODE) |
| credible-sets | Fine-mapping credible set results |
| colocalization | Colocalization analysis results |
| expression | Gene expression data (eQTL, etc.) |
| genes | Gene information, lookups, and nearest genes with coordinates |
| gene-groups | HGNC gene-group membership (`GET /gene_group/members?group_id=\|group_name=[&exclude_olfactory=]` returns all descendant member genes by lineage, with symbol/ensembl/coords; `exclude_olfactory=true` drops olfactory receptors) and symbol normalization (`GET /gene/normalize?symbols=<comma-separated>` resolves approved/alias/previous symbols to current approved HGNC symbols) |
| gene-disease | Gene-disease associations |
| gene-based | Gene-based association results |
| chromatin-peaks | Chromatin accessibility peaks (peak_to_genes includes gene coordinates) |
| exome-results | Exome sequencing association results |
| phenotype | Phenotype descriptions |
| resources | Available resource listing |
| datasets | Dataset configuration |
| rsid | rsID to variant mapping |
| summary-stats | GWAS summary statistics |
| variant-annotation | Variant annotation data (FinnGen, etc.) |

### Data Access Pattern

1. **Configuration-driven**: Each data resource is defined in `app/config/` with paths to GCS-hosted tabix files and column mappings. Dataset metadata (author, version, description, metadata_file, harmonizer type) is loaded from a shared `configs/datasets.yaml` file via `app/config/yaml_loader.py`, which selects the active profile from the `CONFIG_PROFILE` env var and reads the YAML path from `DATASETS_CONFIG_PATH` (default: `./configs/datasets.yaml`). The canonical YAML lives in the suite repo and is synced to each service repo for local dev. Product configs (credible_sets, exome_results, gene_based_results, summary_stats) reference the registry via `dataset_id` and contain only file paths and product-specific settings.
2. **Tabix-based queries**: Genomic region queries use htslib tabix (subprocess) to fetch data from indexed `.tsv.gz` files on GCS. Exome variant results are served for genebass (filtered to mlog10p > 4), IBD exome (resource: `ibd_exome_2026`, 3 phenotypes: IBD, UC, CD, filtered to mlog10p > 4), and SCHEMA2 (schizophrenia, filtered to mlog10p > 4). Data files are in `exome_results4/` on GCS. Individual trait exome variant results can be queried by phenotype via the `exome_results_by_phenotype` endpoint, which uses prefix/suffix patterns to locate per-trait data files. Exome variant columns include `n_cases`/`n_controls` (nullable) and `trait_original`. Gene-based burden results use a separate column schema with `trait_original` and `flags` columns. When several datasets share one combined file (e.g. the external pseudo CS file bundling `covid_hgi`, `pgc_scz`, `pgc_bip`, `gp2_pd` under `credible_sets/ext/`), `DataAccess.stream_range*` dedups by `all_cs_file`/`all_exome_file` so tabix runs once, and `tsv_line_iterator` is given a `resource_filter` that drops shared-file rows belonging to resources not in the request. Per-phenotype individual files live in per-dataset subdirs `ext/individual/<dataset_id>/` so that datasets sharing a resource (e.g. `pgc_scz` + `pgc_bip` → `pgc`) don't collide on phenotype lookups.
3. **Service layer**: `app/services/` contains data access classes (inheriting from `GCloudTabixBase`) that handle tabix queries, parsing, and result formatting. Metadata harmonization uses `build_harmonizer_config(dataset_id)` from the registry to get harmonizer settings. `GeneNameAndPositionMapping` provides gene coordinate lookups (per GENCODE version) used by `SearchIndex` and chromatin peaks streaming to enrich results with genomic positions. `GeneGroupService` (`app/services/gene_group_service.py`) reads the three HGNC gene-group CSVs with polars' `read_csv` (all columns as strings via `infer_schema_length=0`, then ids coerced explicitly) at startup and builds an in-memory map from `hgnc_id` to its FULL gene-group lineage (leaf family plus all ancestor families via the precomputed transitive closure), plus the inverse `group_id -> member hgnc_ids` for descendant resolution. The three inputs are HGNC-native comma-separated CSVs: `gene_has_family` (`hgnc_id`, `family_id` leaf links), `hierarchy_closure` (already-transitive `child_fam_id`/`parent_fam_id`/`distance` rows; only proper ancestors with `distance > 0` are kept, the leaf is unioned in from `gene_has_family`), and `family` (`id` -> `name`, also indexed lowercase for case-insensitive name lookups). File paths come from the active profile's `genes` config keys `gene_has_family_file`, `hierarchy_closure_file`, `family_file` (alongside the existing `hgnc_file`). These three keys are present in BOTH profiles — `app/config/profiles/finngen/genes.py` (under `gs://finngen-commons/results_api_data/mapping_files/`) and `app/config/profiles/daly/genes.py` (under `gs://daly-genetics-results/mapping_files/`) — pointing at `hgnc_gene_has_family.csv`, `hgnc_hierarchy_closure.csv`, and `hgnc_family.csv`, which must be uploaded to each profile's GCS `mapping_files/` directory for the feature to activate. Loading is resilient: if the paths are unconfigured, or a file is missing or unreadable (e.g. not yet uploaded to GCS), a warning is logged and the group map is left empty so startup never fails (mirroring the coordinate-loading try/except in `SearchIndex._load_genes`). Accessors: `group_ids_for_hgnc_id(hgnc_id)`, `groups_for_hgnc_id(hgnc_id)` -> `[(group_id, group_name)]`, `members_of_group(group_id=..., group_name=...)` -> set of member `hgnc_id`s (any leaf/ancestor/root group resolves to all descendant members), `resolve_group_id(group_name)`, `group_name(group_id)`, and `is_loaded()`. Wired through the service container as `gene_group_service` (`create_gene_group_service` in `app/core/service_container.py`) and exposed via `get_gene_group_service()` in `app/dependencies.py`. To turn the `hgnc_id`s returned by `members_of_group` into symbol/ensembl/coordinates, `SearchIndex._load_genes` now also stores each gene record's `hgnc_id` and builds a `genes_by_hgnc_id` lookup (accessor `get_gene_by_hgnc_id(hgnc_id)`); this is the single bridge from HGNC ids back to the symbol/ensembl/coords already loaded from the same HGNC complete-set + GENCODE source (no coordinate logic is duplicated, and chrom keeps the API-wide X=23 convention since it comes from the GENCODE coords lookup). **HGNC id format**: the gene-group files use BARE numeric ids (`3023`) while the complete set (and thus `genes_by_hgnc_id`) uses the prefixed `HGNC:3023` form, so `GeneGroupService` canonicalizes every id to the `HGNC:NNNN` form (via `_canonical_hgnc_id`) when building its maps, and `get_gene_by_hgnc_id` also tolerates the bare form on lookup. Without this, every member resolved to null symbol/coords. The `gene-groups` router (`app/routers/gene_groups.py`, registered in `app/server.py`) joins these two services: it requires exactly one of `group_id`/`group_name` (400 otherwise), resolves a name case-insensitively (404 if unknown, both for an unresolvable name and for an unknown id), and returns `{group_id, group_name, exclude_olfactory, count, members:[{hgnc_id, symbol, ensembl_id, chr, gene_start, gene_end}]}` sorted by symbol (null symbols last). Members lacking a search/coords record are still listed with null fields rather than dropped, and if `is_loaded()` is False (group files not yet uploaded) it logs and returns an empty member list (count 0) instead of erroring. Olfactory receptors are GPCRs that dominate large families (e.g. the G protein-coupled receptor group) by sheer count; the optional `exclude_olfactory` query param (default False on the raw endpoint; the mcp-server tool defaults it to True) drops any member whose lineage contains the `Olfactory receptors` group via `members_of_group(..., exclude_olfactory=True)`. The same router also exposes `GET /gene/normalize?symbols=<comma-separated>`, which resolves each input gene symbol/alias/previous symbol to its current approved HGNC symbol and returns `{mappings:[{input, approved, matched_on}], unresolved:[...]}`. It reuses `SearchIndex.normalize_symbol`, which performs an EXACT case-insensitive dict lookup (built once, lazily, from `self.genes`' approved `symbol` plus merged `aliases`/previous symbols — the free-text gene `name` and ensembl id are deliberately excluded) rather than the fuzzy `search()` path, so normalization never yields ranked near-miss false positives. `matched_on` is `approved` when the input is already an approved symbol, else `alias_or_previous` (the HGNC load merges alias and previous symbols without retaining which list a match came from).
4. **Routing layer**: `app/routers/` defines endpoints that validate input, call services, and return formatted responses

### Authentication

- JWT-based authentication with `@is_public` decorator for public endpoints
- Auth dependency applied globally via `dependencies=[Depends(auth_required)]`
- Health check (`/healthz`) and root (`/api/v1`) are public

### Testing

- Integration tests in `tests/` that run against a live server instance
- Configurable server URL via `--server-url` pytest option (default: `http://localhost:4000`)
- Tests use `requests` library to make HTTP calls to the running server
- Run with: `pytest` or `pytest --server-url http://host:port`
- A few self-contained unit tests also live in `tests/` (e.g. `test_gene_group_service.py`, which writes small CSV fixtures and exercises `GeneGroupService` directly); these need neither a live server nor GCS access and run under plain `pytest`

### Deployment

- Docker container built with htslib (for tabix) and gcloud SDK
- GCS authentication via Workload Identity or service account key. `GCloudTabixBase.ensure_gcs_token()` fetches/refreshes the OAuth token used by tabix subprocesses (exported as `GCS_OAUTH_TOKEN`); the whole check-and-refresh (including first init) holds `_gcs_token_lock` so concurrent first calls can't race on the env var. htslib reports transient GCS access failures (timed-out/partial HTTPS requests) as misleading `Invalid argument` / `No such file or directory` / index-load errors and never retries them, so `_get_header` and `_stream_range` retry tabix up to `_TABIX_MAX_ATTEMPTS` (3) times with exponential backoff; range streaming only retries while no bytes have been yielded yet (early index-load/open failures), and an empty-but-successful result is never treated as an error. The aiohttp GCS client (used only by `_stream_file`) is created lazily via `_ensure_storage()` on first streaming use rather than eagerly in `__init__`, so objects used solely for tabix (the whole credible-set/coloc path) no longer leak an unclosed aiohttp session.
- Default port: 4000
- Entry point: `/opt/genetics-results-api/start.sh` which initializes gcloud auth and starts uvicorn
