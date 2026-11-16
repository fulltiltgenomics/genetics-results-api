"""A GeneGroupService over small HGNC-native CSV fixtures written to a temp dir."""

from app.services.gene_group_service import GeneGroupService

# hierarchy: 100 (root "Receptors") -> 110 ("GPCRs") -> 170 ("5-HT1 receptors")
#            110 ("GPCRs") -> 180 ("Olfactory receptors")
#            100 -> 120 ("Ion channels")
# genes:
#   HGNC:5286 (HTR1A) leaf = 170 (deep child)
#   HGNC:5287 (HTR1B) leaf = 170
#   HGNC:9999 (CHAN1) leaf = 120
#   HGNC:8000 (OR1A1) leaf = 180 (olfactory)
FAMILY_CSV = '''id,name,abbreviation
100,"Receptors","RCPT"
110,"GPCRs","GPCR"
120,"Ion channels","ICH"
170,"5-HT1 receptors","HTR1"
180,"Olfactory receptors","OR"
'''

# closure is already transitive and includes proper ancestors only (distance>0)
HIERARCHY_CLOSURE_CSV = '''parent_fam_id,child_fam_id,distance
100,110,1
100,170,2
110,170,1
100,120,1
100,180,2
110,180,1
'''

# NOTE: real hgnc_gene_has_family.csv uses BARE numeric hgnc ids ("5286"),
# while the HGNC complete set / search index use the prefixed "HGNC:5286" form.
# The service must canonicalize these to the prefixed form (asserted below).
GENE_HAS_FAMILY_CSV = '''hgnc_id,family_id
5286,170
5287,170
9999,120
8000,180
'''


def make_gene_group_service(tmp_path) -> GeneGroupService:
    fam = tmp_path / "hgnc_family.csv"
    closure = tmp_path / "hgnc_hierarchy_closure.csv"
    ghf = tmp_path / "hgnc_gene_has_family.csv"
    fam.write_text(FAMILY_CSV)
    closure.write_text(HIERARCHY_CLOSURE_CSV)
    ghf.write_text(GENE_HAS_FAMILY_CSV)
    return GeneGroupService(
        gene_has_family_file=str(ghf),
        hierarchy_closure_file=str(closure),
        family_file=str(fam),
    )
