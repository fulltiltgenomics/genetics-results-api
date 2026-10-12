"""Column mapping for the FinnGen R14 multi-cohort (FinnGen+UKBB+MVP+AoU) meta-analysis.

The contributing cohorts vary per phenotype, so this is a static superset of every
cohort/ancestry column; the serving layer intersects it against each file's own header
(app.core.streams.union_output_columns), and a column one file lacks is served as NA.

Differences from the R13 meta files that the mapping has to absorb:
- tabix -H strips the leading '#', so the first column arrives as ``CHR``.
- UKBB is ``ukbb_eur_*`` rather than ``ukbb_*``.
- leave-one-out blocks are ``leave_cohort_<family>_*`` at cohort-family granularity
  and also carry ``_N`` and ``_het_p``.
- a real ``rsid`` column exists, so the ``SNP`` chr:pos:ref:alt marker the R13 mapping
  served as ``rsid`` is dropped rather than mapped onto the same output name.
"""

_COHORTS = [
    "fg",
    "aou_eur", "aou_afr", "aou_amr", "aou_eas", "aou_sas", "aou_mid",
    "mvp_eur", "mvp_afr", "mvp_amr",
    "ukbb_eur",
]
_MVP_COHORTS = [c for c in _COHORTS if c.startswith("mvp_")]
_LEAVE = ["fg", "aou", "mvp", "ukbb"]


def _build_mapping() -> dict[str, str]:
    m: dict[str, str] = {
        "CHR": "chr",
        "POS": "pos",
        "REF": "ref",
        "ALT": "alt",
    }
    for c in _COHORTS:
        m[f"{c}_beta"] = f"{c}_beta"
        m[f"{c}_sebeta"] = f"{c}_se"
        m[f"{c}_pval"] = f"{c}_pval"
        m[f"{c}_af_alt"] = f"{c}_af"
    # MVP's imputation quality is the only way to judge whether an MVP estimate that
    # disagrees with the others is a poorly imputed variant
    for c in _MVP_COHORTS:
        m[f"{c}_r2"] = f"{c}_r2"
    m.update({
        "all_meta_N": "meta_n",
        "all_inv_var_meta_beta": "beta",
        "all_inv_var_meta_sebeta": "se",
        "all_inv_var_meta_p": "pval",
        "all_inv_var_meta_mlogp": "mlog10p",
        "all_inv_var_het_p": "het_p",
    })
    for c in _LEAVE:
        src = f"leave_cohort_{c}"
        m[f"{src}_N"] = f"leave_{c}_n"
        m[f"{src}_inv_var_meta_beta"] = f"leave_{c}_beta"
        m[f"{src}_inv_var_meta_sebeta"] = f"leave_{c}_se"
        m[f"{src}_inv_var_meta_p"] = f"leave_{c}_pval"
        m[f"{src}_inv_var_meta_mlogp"] = f"leave_{c}_mlog10p"
        m[f"{src}_inv_var_meta_het_p"] = f"leave_{c}_het_p"
    m["rsid"] = "rsid"
    return m


R14_META_COLUMN_MAPPING = _build_mapping()
