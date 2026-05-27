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
2. **Tabix-based queries**: Genomic region queries use htslib tabix (subprocess) to fetch data from indexed `.tsv.gz` files on GCS. Exome variant results are served for genebass (filtered to mlog10p > 4), IBD exome (resource: `ibd_exome_2026`, 3 phenotypes: IBD, UC, CD, filtered to mlog10p > 4), and SCHEMA2 (schizophrenia, filtered to mlog10p > 4). Data files are in `exome_results4/` on GCS. Individual trait exome variant results can be queried by phenotype via the `exome_results_by_phenotype` endpoint, which uses prefix/suffix patterns to locate per-trait data files. Exome variant columns include `n_cases`/`n_controls` (nullable) and `trait_original`. Gene-based burden results use a separate column schema with `trait_original` and `flags` columns. When several datasets share one combined file (e.g. the external pseudo CS file bundling `covid_hgi`, `pgc_meta`, `gp2_meta` under `credible_sets/ext/`), `DataAccess.stream_range*` dedups by `all_cs_file`/`all_exome_file` so tabix runs once, and `tsv_line_iterator` is given a `resource_filter` that drops shared-file rows belonging to resources not in the request.
3. **Service layer**: `app/services/` contains data access classes (inheriting from `GCloudTabixBase`) that handle tabix queries, parsing, and result formatting. Metadata harmonization uses `build_harmonizer_config(dataset_id)` from the registry to get harmonizer settings. `GeneNameAndPositionMapping` provides gene coordinate lookups (per GENCODE version) used by `SearchIndex` and chromatin peaks streaming to enrich results with genomic positions.
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

### Deployment

- Docker container built with htslib (for tabix) and gcloud SDK
- GCS authentication via Workload Identity or service account key
- Default port: 4000
- Entry point: `/opt/genetics-results-api/start.sh` which initializes gcloud auth and starts uvicorn
