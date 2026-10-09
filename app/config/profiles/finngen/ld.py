"""FinnGen profile: LD panels served from the suite's own matrices.

Empty until the R12 matrices are staged in finngen-commons: the startup check verifies
every file an entry names and refuses to start otherwise, so an entry cannot precede the
files. Copy the daly entry (app/config/profiles/daly/ld.py) with the finngen path once
`ld/finngen/r12/finngen_r12_chr{chrom}_ld.tsv.gz` and its `.tbi` files are in place.
Every LD request on this profile goes to the upstream proxy until then.
"""

ld_panels: list[dict] = []
