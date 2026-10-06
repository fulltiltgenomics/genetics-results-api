"""Startup validation that every configured data file is reachable.

Run before the server accepts traffic so a missing/misconfigured data file fails
startup loudly instead of surfacing as a 500 on the first request that touches it.

Two kinds of checks run concurrently in a thread pool and ALL failures are
collected before raising, so one run reports every problem at once:

  - tabix header check (`tabix -H`) for every fixed/combined tabix-indexed file.
    This also proves the .tbi/.csi index loads, not just that the object exists.
    For the credible-set files the header read back is also compared column by
    column against the schema-derived header (see app.config.credible_sets), since
    a cross-resource response is emitted positionally under the first file's header.
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
# 32 lets the ~65 fixed files clear in ~2 waves without spawning one subprocess per
# file at once (per-call latency, not worker count, is the real floor here).
_MAX_WORKERS = 32


TabixCheck = tuple[str, str, list[bytes] | None]


def _collect_tabix_files() -> list[TabixCheck]:
    """Collect (label, gs_path, expected_header) for every fixed/combined tabix file.

    expected_header is None where only readability is checked. Per-phenotype
    prefix/suffix files are intentionally excluded (see module docstring). Paths shared
    by several datasets (e.g. the EXT combined credible-set file) are deduped so tabix
    runs once per unique file.
    """
    import app.config.common as common
    from app.config.chromatin_peaks import chromatin_peaks_data
    from app.config.coloc import coloc
    from app.config.credible_sets import cs_file_header, cs_qtl_file_header
    from app.config.credible_sets import data_files as cs_files
    from app.config.exome_results import exome_data_files
    from app.config.expression import expression_data
    from app.config.gene_based_results import gene_based_data_files
    from app.config.mpra import mpra_data
    from app.config.open_chromatin import open_chromatin_data
    from app.config.summary_stats import data_files as sumstats_files
    from app.config.variant_effect import variant_effect_data

    files: list[TabixCheck] = []

    for df in cs_files:
        cfg = df.get("cs", {})
        if "all_cs_file" in cfg:
            files.append((f"cs:{df['id']}", cfg["all_cs_file"], cs_file_header))
        if "all_cs_qtl_file" in cfg:
            files.append(
                (f"cs_qtl:{df['id']}", cfg["all_cs_qtl_file"], cs_qtl_file_header)
            )

    unchecked: list[tuple[str, str]] = []

    for df in exome_data_files:
        cfg = df.get("exome", {})
        if "all_exome_file" in cfg:
            unchecked.append((f"exome:{df['id']}", cfg["all_exome_file"]))

    for df in gene_based_data_files:
        cfg = df.get("gene_based", {})
        if "file" in cfg:
            unchecked.append((f"gene_based:{df['id']}", cfg["file"]))

    for c in coloc:
        if "credset_file" in c:
            unchecked.append((f"coloc_credset:{c['name']}", c["credset_file"]))
        if "coloc_file" in c:
            unchecked.append((f"coloc:{c['name']}", c["coloc_file"]))

    # only the fixed single-file sumstats entries; prefix/suffix ones are per-phenotype
    for df in sumstats_files:
        if "file" in df:
            unchecked.append((f"sumstats:{df['id']}", df["file"]))

    for d in expression_data:
        unchecked.append((f"expression:{d['resource']}", d["file"]))

    for d in chromatin_peaks_data:
        unchecked.append((f"chromatin_peaks:{d['resource']}", d["file"]))
        if "file_by_gene" in d:
            unchecked.append(
                (f"chromatin_peaks_by_gene:{d['resource']}", d["file_by_gene"])
            )

    for d in open_chromatin_data:
        unchecked.append((f"open_chromatin:{d['resource']}", d["file"]))

    # variant_effect/mpra config is per-DATASET, not per-resource: the marderstein
    # resource ships two predictor files (chrombpnet, flare), so key on dataset_id
    for d in variant_effect_data:
        unchecked.append((f"variant_effect:{d['dataset_id']}", d["file"]))

    for d in mpra_data:
        unchecked.append((f"mpra:{d['dataset_id']}", d["file"]))

    for source, cfg in common.variant_annotation_sources.items():
        unchecked.append((f"variant_annotation:{source}", cfg["file"]))

    unchecked.append(("gnomad", common.gnomad["file"]))
    unchecked.append(("rsid_db", common.rsid_db["file"]))

    files.extend((label, path, None) for label, path in unchecked)

    seen: set[str] = set()
    deduped: list[TabixCheck] = []
    for label, path, expected in files:
        if path not in seen:
            seen.add(path)
            deduped.append((label, path, expected))
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
        ("genes:gene_name_mapping_file", genes["gene_name_mapping_file"]),
        ("genes:hgnc_file", genes["hgnc_file"]),
    ]
    template = genes["gene_position_file_template"]
    for version in genes["gencode_versions"]:
        files.append(
            (f"genes:gencode_v{version}", template.format(version=version))
        )
    for version, path in genes.get("exon_file_by_version", {}).items():
        files.append((f"genes:exons_v{version}", path))

    for key, cfg in gene_disease.items():
        if isinstance(cfg, dict) and "file" in cfg:
            files.append((f"gene_disease:{key}", cfg["file"]))

    # curated variant sets served by the variant_set router
    for name, cfg in config.variant_set_files.items():
        if isinstance(cfg, dict) and "file" in cfg:
            files.append((f"variant_set:{name}", cfg["file"]))

    return files


def _header_mismatch(actual: list[bytes], expected: list[bytes]) -> str | None:
    """Describe the first way `actual` departs from `expected`, or None if identical.

    Names the position and both column names so the operator can tell a renamed column
    from a shifted one without opening the file.
    """
    for i, (a, e) in enumerate(zip(actual, expected)):
        if a != e:
            return f"column {i + 1} is {a.decode()!r}, expected {e.decode()!r}"
    if len(actual) < len(expected):
        missing = [c.decode() for c in expected[len(actual):]]
        return f"missing trailing column(s) {missing}"
    if len(actual) > len(expected):
        extra = [c.decode() for c in actual[len(expected):]]
        return f"unexpected trailing column(s) {extra}"
    return None


def _check_tabix_header(
    tabix: GCloudTabixBase,
    label: str,
    gs_path: str,
    expected_header: list[bytes] | None = None,
) -> str | None:
    """`tabix -H` one file (with the base class's built-in retry). Error string or None.

    With `expected_header`, the header read back must match it exactly.
    """
    try:
        header = tabix._get_header(gs_path)
    except Exception as e:
        return f"{label}: tabix header failed for {gs_path}: {e}"
    if expected_header is None:
        return None
    mismatch = _header_mismatch(header, expected_header)
    if mismatch:
        return f"{label}: header mismatch in {gs_path}: {mismatch}"
    return None


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
    listing them if any file is missing, unreadable or carries a header other than
    the one its family expects. Returns normally otherwise.
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
            pool.submit(_check_tabix_header, tabix, label, path, expected)
            for label, path, expected in tabix_files
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
            f"{len(errors)} configured data file(s) missing, unreadable or "
            "mis-headed:\n  - "
            + "\n  - ".join(errors)
        )

    logger.info("All configured data files verified")
