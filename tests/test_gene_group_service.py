"""
Unit tests for GeneGroupService (HGNC gene-group + lineage loading).

These are self-contained unit tests: they write small HGNC-native CSV
fixtures to a temp dir and pass the paths directly to the service, so they
need neither a live server nor GCS access.
"""

import pytest

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


@pytest.fixture
def service(tmp_path):
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


def test_full_lineage_includes_leaf_and_all_ancestors(service):
    assert service.is_loaded()
    # HTR1A's leaf is the deep child 170; lineage must include ancestors 110 and root 100
    assert service.group_ids_for_hgnc_id("HGNC:5286") == {170, 110, 100}


def test_lineage_resolves_group_names(service):
    groups = dict(service.groups_for_hgnc_id("HGNC:5286"))
    assert groups[170] == "5-HT1 receptors"
    assert groups[110] == "GPCRs"
    assert groups[100] == "Receptors"


def test_members_resolve_to_canonical_prefixed_ids(service):
    # bare ids in gene_has_family must come out canonicalized to 'HGNC:NNNN'
    # so they resolve against the search index (which keys on the prefixed form)
    assert service.group_ids_for_hgnc_id("5286") == {170, 110, 100}
    assert service.members_of_group(group_id=170) == {"HGNC:5286", "HGNC:5287"}


def test_members_of_root_group_returns_all_descendants(service):
    # root group 100 contains every gene below it in the hierarchy
    assert service.members_of_group(group_id=100) == {
        "HGNC:5286",
        "HGNC:5287",
        "HGNC:9999",
        "HGNC:8000",
    }


def test_members_of_intermediate_group(service):
    # GPCRs (110) covers the 5-HT1 genes and the olfactory gene, not ion channels
    assert service.members_of_group(group_id=110) == {
        "HGNC:5286",
        "HGNC:5287",
        "HGNC:8000",
    }


def test_exclude_olfactory_drops_olfactory_members(service):
    # the olfactory gene (8000) is dropped; the other GPCRs remain
    assert service.members_of_group(group_id=110, exclude_olfactory=True) == {
        "HGNC:5286",
        "HGNC:5287",
    }
    # also works via group name
    assert service.members_of_group(
        group_name="GPCRs", exclude_olfactory=True
    ) == {"HGNC:5286", "HGNC:5287"}
    # default keeps them
    assert "HGNC:8000" in service.members_of_group(group_id=110)


def test_members_of_leaf_group(service):
    assert service.members_of_group(group_id=170) == {"HGNC:5286", "HGNC:5287"}
    assert service.members_of_group(group_id=120) == {"HGNC:9999"}


def test_members_by_group_name_case_insensitive(service):
    assert service.members_of_group(group_name="receptors") == {
        "HGNC:5286",
        "HGNC:5287",
        "HGNC:9999",
        "HGNC:8000",
    }
    assert service.resolve_group_id("GPCRs") == 110


def test_unknown_hgnc_id_and_group(service):
    assert service.group_ids_for_hgnc_id("HGNC:0000") == set()
    assert service.members_of_group(group_id=999) == set()
    assert service.members_of_group(group_name="nope") == set()
    assert service.group_name(999) is None


def test_missing_files_do_not_crash_and_leave_map_empty(tmp_path):
    # mirrors resilient startup: unreadable/missing files -> empty map, no raise
    svc = GeneGroupService(
        gene_has_family_file=str(tmp_path / "missing_ghf.csv"),
        hierarchy_closure_file=str(tmp_path / "missing_closure.csv"),
        family_file=str(tmp_path / "missing_family.csv"),
    )
    assert not svc.is_loaded()
    assert svc.group_ids_for_hgnc_id("HGNC:5286") == set()
    assert svc.members_of_group(group_id=100) == set()
