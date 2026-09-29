"""FinnGen profile: the sandbox custom GWAS folders served straight from library-green.

One entry per FinnGen release. The unmodifiable REGENIE pipeline writes every run under
`{prefix}<run>/`, and the fine-mapping and HLA pipelines write into the same folder, so
nothing here is staged or munged: results-api lists the prefix and reads the pipeline's
own files (see app/services/custom_gwas_catalog.py for the layout it expects).

Releases are separate resources rather than versions of one, because a run name is only
unique within a release (79 R14 names also exist under R13) and 49 R14 names are core
endpoint codes, so they cannot share the `finngen` resource either.
"""

_BUCKET = "finngen-production-library-green"

releases = [
    {
        "id": "finngen_custom_r14",
        "resource": "finngen_custom_r14",
        "bucket": _BUCKET,
        "prefix": "finngen_R14/sandbox_custom_gwas/",
        # the `dataset` column of served credible-set rows
        "dataset_label": "FinnGen_R14_custom",
        "gwas_dataset_id": "finngen_custom_r14_gwas",
        "hla_dataset_id": "finngen_custom_r14_hla",
        # the current release still receives runs: a name the last listing did not see
        # resolves to the pipeline's `<name>/<name>.gz` convention and is HEAD-checked,
        # so a run is queryable the moment it lands rather than at the next refresh
        "convention_fallback": True,
        "refresh_seconds": 900,
    },
    {
        "id": "finngen_custom_r13",
        "resource": "finngen_custom_r13",
        "bucket": _BUCKET,
        "prefix": "finngen_R13/sandbox_custom_gwas/",
        "dataset_label": "FinnGen_R13_custom",
        "gwas_dataset_id": "finngen_custom_r13_gwas",
        # closed release: the R13 pipeline wrote no HLA results, and a folder there is a
        # run title that may hold several phenotypes, so only the listing resolves a name
        "convention_fallback": False,
        "refresh_seconds": 6 * 3600,
    },
    {
        "id": "finngen_custom_r12",
        "resource": "finngen_custom_r12",
        "bucket": _BUCKET,
        "prefix": "finngen_R12/sandbox_custom_gwas/",
        "dataset_label": "FinnGen_R12_custom",
        "gwas_dataset_id": "finngen_custom_r12_gwas",
        "convention_fallback": False,
        "refresh_seconds": 6 * 3600,
    },
]
