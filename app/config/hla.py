"""
Classical HLA allele association configuration.

HLA results are stored exactly like summary statistics — one bgzip+tabix TSV per
phenotype — and are therefore registered in the profile ``summary_stats`` config under
``data_type: "hla"`` and read by ``SumstatsDataAccess``. What this module adds is the
locus registry the sumstats machinery has no notion of: which HLA genes exist, where
their alleles are anchored, and how wide the whole imputed region is.

Query model: the unit of association is an imputed classical HLA **allele**
(``B*27:05``), not a nucleotide variant. Every allele of a gene is written at that
gene's single anchor position, so a per-phenotype file is ~187 rows spanning
``HLA_REGION_START..HLA_REGION_END`` and one range read returns the trait's complete
HLA profile. A variant-style ``chrom_pos_ref_alt`` lookup cannot address an allele and
is deliberately not offered.

Cross-phenotype questions ("which traits is ``B*27:05`` associated with?") span every
per-phenotype file and are not answerable here — they go through the BigQuery
``hla_associations_v`` view, which holds the same rows in one table.
"""

HLA_CHROM = 6

# Anchor position of each HLA gene's alleles, taken from the source files rather than
# from a gene annotation: the imputation pipeline writes every allele of a gene at one
# position, so these ARE the coordinates the tabix index is built on.
#
# DRB3/DRB4/DRB5 share the placeholder 32500000 — the pipeline gives the secondary DRB
# loci one synthetic anchor instead of their true coordinates. A positional query at
# that anchor therefore cannot separate them; filter on `gene` to do that.
HLA_GENE_POSITIONS: dict[str, int] = {
    "HLA-A": 29941260,
    "HLA-C": 31268750,
    "HLA-B": 31269490,
    "HLA-DRB3": 32500000,
    "HLA-DRB4": 32500000,
    "HLA-DRB5": 32500000,
    "HLA-DRB1": 32578770,
    "HLA-DQA1": 32628180,
    "HLA-DQB1": 32659470,
    "HLA-DPB1": 33075990,
}

HLA_REGION_START = min(HLA_GENE_POSITIONS.values())
HLA_REGION_END = max(HLA_GENE_POSITIONS.values())

# the data_type these datasets are registered under in the profile summary_stats config
HLA_DATA_TYPE = "hla"


def gene_positions() -> list[dict]:
    """The HLA gene registry, ordered by position, for the /hla/genes endpoint."""
    return [
        {"gene": gene, "chrom": HLA_CHROM, "pos": pos}
        for gene, pos in sorted(HLA_GENE_POSITIONS.items(), key=lambda kv: (kv[1], kv[0]))
    ]
