"""Column mappings for the FinnGen R12 sex-specific and recessive GWAS releases.

Both are served straight from the green library, shared by the finngen and daly profiles.

Sex-specific: the per-endpoint female-male meta-analysis files
(``sex_difference/meta_analysis/meta_summary_stats/<PHENO>_meta_out_filtered.tsv.gz``)
carry each sex's own REGENIE result beside the inverse-variance combination and a
heterogeneity p-value, in the same layout as the pQTL and R14 meta files, so the
``all_inv_var_*`` block maps to the house names the same way. ``SNP`` is the
chr:pos:ref:alt marker and is dropped because a real ``rsid`` column exists.

Recessive: ``summary_stats_recessive/results/<PHENO>.gz`` holds the recessive test under
FinnGen's native names and the additive test of the same variant under ``*_add``. The
unsuffixed statistics are therefore the recessive ones, and the additive ones are mapped
with an ``additive_`` prefix. ``effect_allele``/``flipped`` matter for sign: where the alt
allele frequency exceeded 0.5 the reference allele was the tested effect allele, so a
flipped row's recessive beta is on the opposite allele from its additive beta.
"""

SEX_SPECIFIC_COLUMN_MAPPING = {
    "CHR": "chr",
    "POS": "pos",
    "REF": "ref",
    "ALT": "alt",
    "female_beta": "female_beta",
    "female_sebeta": "female_se",
    "female_pval": "female_pval",
    "female_af_alt": "female_af",
    "female_af_alt_cases": "female_af_cases",
    "female_af_alt_controls": "female_af_controls",
    "male_beta": "male_beta",
    "male_sebeta": "male_se",
    "male_pval": "male_pval",
    "male_af_alt": "male_af",
    "male_af_alt_cases": "male_af_cases",
    "male_af_alt_controls": "male_af_controls",
    "all_meta_N": "meta_n",
    "all_inv_var_meta_beta": "beta",
    "all_inv_var_meta_sebeta": "se",
    "all_inv_var_meta_p": "pval",
    "all_inv_var_meta_mlogp": "mlog10p",
    "all_inv_var_het_p": "het_p",
    "rsid": "rsid",
}

RECESSIVE_COLUMN_MAPPING = {
    "chrom": "chr",
    "pos": "pos",
    "ref": "ref",
    "alt": "alt",
    "pval": "pval",
    "mlogp": "mlog10p",
    "beta": "beta",
    "sebeta": "se",
    "effect_allele": "effect_allele",
    "flipped": "flipped",
    "pval_add": "additive_pval",
    "mlogp_add": "additive_mlog10p",
    "beta_add": "additive_beta",
    "sebeta_add": "additive_se",
    "mlogp_diff": "mlog10p_diff",
    "af_alt": "af",
    "af_alt_cases": "af_cases",
    "af_alt_controls": "af_controls",
    "INFO": "info",
    "AC_Het": "ac_het",
    "AC_Hom": "ac_hom",
    "most_severe": "most_severe",
    "gene_most_severe": "gene_most_severe",
    "rsid": "rsid",
}
