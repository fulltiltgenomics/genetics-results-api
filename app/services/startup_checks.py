"""Startup validation that every configured data file is reachable.

Run before the server accepts traffic so a missing/misconfigured data file fails
startup loudly instead of surfacing as a 500 on the first request that touches it.

Two kinds of checks run concurrently in a thread pool and ALL failures are
collected before raising, so one run reports every problem at once:

  - tabix header check (`tabix -H`) for every fixed/combined tabix-indexed file.
    This also proves the .tbi/.csi index loads, not just that the object exists.
  - existence check (fsspec) for the non-tabix mapping files.

Per-phenotype files addressed by `prefix` + <phenotype> + `suffix` are NOT
enumerable statically (thousands per resource, set unknown), so they are not
header-checked here. The end-to-end range smoke query in app.server's lifespan
exercises the combined cross-resource query path instead.
"""

import concurrent.futures
import logging

import fsspec

from app.services.gcloud_tabix_base import GCloudTabixBase

logger = logging.getLogger(__name__)

# tabix -H opens a network connection (and subprocess) per file; bound the fan-out.
# 32 lets the ~55 fixed files clear in ~2 waves without spawning one subprocess per
# file at once (per-call latency, not worker count, is the real floor here).
_MAX_WORKERS = 32


def _collect_tabix_files() -> list[tuple[str, str]]:
    """Collect (label, gs_path) for every fixed/combined tabix-indexed file in config.

    Per-phenotype prefix/suffix files are intentionally excluded (see module docstring).
    Paths shared by several datasets (e.g. the EXT combined credible-set file) are
    deduped so tabix runs once per unique file.
    """
    import app.config.common as common
    from app.config.chromatin_peaks import chromatin_peaks_data
    from app.config.coloc import coloc
    from app.config.credible_sets import data_files as cs_files
    from app.config.exome_results import exome_data_files
    from app.config.expression import expression_data
    from app.config.gene_based_results import gene_based_data_files
    from app.config.summary_stats import data_files as sumstats_files

    files: list[tuple[str, str]] = []

    for df in cs_files:
        cfg = df.get("cs", {})
        if "all_cs_file" in cfg:
            files.append((f"cs:{df['id']}", cfg["all_cs_file"]))
        if "all_cs_qtl_file" in cfg:
            files.append((f"cs_qtl:{df['id']}", cfg["all_cs_qtl_file"]))

    for df in exome_data_files:
        cfg = df.get("exome", {})
        if "all_exome_file" in cfg:
            files.append((f"exome:{df['id']}", cfg["all_exome_file"]))

    for df in gene_based_data_files:
        cfg = df.get("gene_based", {})
        if "file" in cfg:
            files.append((f"gene_based:{df['id']}", cfg["file"]))

    for c in coloc:
        if "credset_file" in c:
            files.append((f"coloc_credset:{c['name']}", c["credset_file"]))
        if "coloc_file" in c:
            files.append((f"coloc:{c['name']}", c["coloc_file"]))

    # only the fixed single-file sumstats entries; prefix/suffix ones are per-phenotype
    for df in sumstats_files:
        if "file" in df:
            files.append((f"sumstats:{df['id']}", df["file"]))

    for d in expression_data:
        files.append((f"expression:{d['resource']}", d["file"]))

    for d in chromatin_peaks_data:
        files.append((f"chromatin_peaks:{d['resource']}", d["file"]))

    for source, cfg in common.variant_annotation_sources.items():
        files.append((f"variant_annotation:{source}", cfg["file"]))

    files.append(("gnomad", common.gnomad["file"]))
    files.append(("rsid_db", common.rsid_db["file"]))

    seen: set[str] = set()
    deduped: list[tuple[str, str]] = []
    for label, path in files:
        if path not in seen:
            seen.add(path)
            deduped.append((label, path))
    return deduped


def _collect_mapping_files() -> list[tuple[str, str]]:
    """Collect (label, gs_path) for non-tabix mapping files to existence-check.

    Excludes the gene-group CSVs (loaded resiliently by design — may not be uploaded
    yet) and the phenotype-markdown template (a per-request {resource}/{phenocode} path).
    """
    import app.config.common as config
    from app.config.gene_disease import gene_disease
    from app.config.genes import genes

    files: list[tuple[str, str]] = [
        ("genes:model_file", genes["model_file"]),
        ("genes:gene_name_mapping_file", genes["gene_name_mapping_file"]),
        ("genes:hgnc_file", genes["hgnc_file"]),
    ]
    template = genes["gene_position_file_template"]
    for version in genes["gencode_versions"]:
        files.append(
            (f"genes:gencode_v{version}", template.format(version=version))
        )

    for key, cfg in gene_disease.items():
        if isinstance(cfg, dict) and "file" in cfg:
            files.append((f"gene_disease:{key}", cfg["file"]))

    # curated variant sets served by the variant_set router
    for name, cfg in config.variant_set_files.items():
        if isinstance(cfg, dict) and "file" in cfg:
            files.append((f"variant_set:{name}", cfg["file"]))

    return files


def _check_tabix_header(tabix: GCloudTabixBase, label: str, gs_path: str) -> str | None:
    """`tabix -H` one file (with the base class's built-in retry). Error string or None."""
    try:
        tabix._get_header(gs_path)
        return None
    except Exception as e:
        return f"{label}: tabix header failed for {gs_path}: {e}"


def _check_exists(label: str, path: str) -> str | None:
    """Check a file exists via fsspec. Error string or None."""
    try:
        fs, _, paths = fsspec.get_fs_token_paths(path)
        if not fs.exists(paths[0]):
            return f"{label}: file does not exist: {path}"
        return None
    except Exception as e:
        return f"{label}: error checking {path}: {e}"


def verify_all_data_files() -> None:
    """Verify every configured tabix file and mapping file is reachable.

    Runs all checks concurrently, collects every failure, and raises RuntimeError
    listing them if any file is missing/unreadable. Returns normally otherwise.
    """
    tabix_files = _collect_tabix_files()
    mapping_files = _collect_mapping_files()
    logger.info(
        f"Verifying {len(tabix_files)} tabix files and "
        f"{len(mapping_files)} mapping files on startup"
    )

    # one shared instance is enough: _get_header is stateless across files and the
    # aiohttp session stays unused (header access goes through the tabix subprocess)
    tabix = GCloudTabixBase()

    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = [
            pool.submit(_check_tabix_header, tabix, label, path)
            for label, path in tabix_files
        ] + [
            pool.submit(_check_exists, label, path)
            for label, path in mapping_files
        ]
        for future in concurrent.futures.as_completed(futures):
            err = future.result()
            if err:
                errors.append(err)

    if errors:
        errors.sort()
        raise RuntimeError(
            f"{len(errors)} configured data file(s) missing or unreadable:\n  - "
            + "\n  - ".join(errors)
        )

    logger.info("All configured data files verified")
