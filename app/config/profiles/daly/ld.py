"""Daly profile: LD panels served from the suite's own matrices.

One entry per panel. `fields` lists the statistics the files carry after the four key
columns (`chr pos variant1 variant2`), in file order; the startup header check proves every
chromosome file matches exactly, so a file with a renamed or extra column refuses to serve
rather than returning one statistic under another's name. `r2_floor` and `max_distance`
are the cut-offs the matrices were computed with (plink2 `--ld-window-r2` and
`--ld-window-kb`): a pair absent from the files is below the floor or beyond the cap,
and every response states both so a caller can tell "independent" from "not computed".
"""

ld_panels = [
    {
        "panel": "finngen_r12",
        "data_source": "gcloud",
        "file_template": "gs://daly-genetics-results/ld/finngen/r12/finngen_r12_chr{chrom}_ld.tsv.gz",
        "chromosomes": [*map(str, range(1, 23)), "X"],
        "fields": ("r", "r2"),
        "r2_floor": 0.01,
        "max_distance": 3_000_000,
        "n_samples": 520_210,
        "description": (
            "In-sample LD of the FinnGen R12 release (520,210 samples), plink2 --r-unphased: "
            "signed r and r2 for every pair within 3 Mb at r2 >= 0.01, both orderings. "
            "Not a reference panel: this is the LD of the GWAS samples themselves."
        ),
    },
]
