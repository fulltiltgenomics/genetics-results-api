"""
HGNC gene-group + lineage service.

Loads the three HGNC gene-group files (gene_has_family, hierarchy_closure,
family) and builds an in-memory map from hgnc_id to its FULL gene-group
lineage (the leaf family plus all ancestor families via the precomputed
transitive closure), with group names resolved from the family table.

Loading is resilient: if any file is missing or unreadable (e.g. not yet
uploaded to GCS), a warning is logged and the group map is left empty so the
service still starts. This mirrors the coordinate-loading try/except in
SearchIndex._load_genes.
"""

import logging
from collections import defaultdict

import polars as pl

from app.config.genes import genes

logger = logging.getLogger(__name__)


class GeneGroupService:
    def __init__(
        self,
        gene_has_family_file: str | None = None,
        hierarchy_closure_file: str | None = None,
        family_file: str | None = None,
    ) -> None:
        self._gene_has_family_file = (
            gene_has_family_file
            if gene_has_family_file is not None
            else genes.get("gene_has_family_file")
        )
        self._hierarchy_closure_file = (
            hierarchy_closure_file
            if hierarchy_closure_file is not None
            else genes.get("hierarchy_closure_file")
        )
        self._family_file = (
            family_file if family_file is not None else genes.get("family_file")
        )

        # hgnc_id -> set of group ids spanning the full lineage (leaf + ancestors)
        self._lineage_by_hgnc_id: dict[str, set[int]] = {}
        # group id -> set of hgnc_ids whose lineage contains that group
        self._members_by_group_id: dict[int, set[str]] = defaultdict(set)
        # group id -> group name
        self._group_names: dict[int, str] = {}
        # lowercase group name -> group id (for name-based lookups)
        self._group_id_by_name: dict[str, int] = {}

        self._load()

    def _load(self) -> None:
        try:
            self._build()
        except Exception as e:
            logger.warning(
                f"Could not load HGNC gene-group data; gene groups disabled: {e}"
            )
            self._lineage_by_hgnc_id = {}
            self._members_by_group_id = defaultdict(set)
            self._group_names = {}
            self._group_id_by_name = {}

    def _build(self) -> None:
        if not (
            self._gene_has_family_file
            and self._hierarchy_closure_file
            and self._family_file
        ):
            logger.warning(
                "HGNC gene-group file paths not configured; gene groups disabled"
            )
            return

        # read all columns as strings to avoid schema-inference surprises with
        # quoted HGNC-native CSVs, then coerce ids explicitly
        gene_has_family = pl.read_csv(
            self._gene_has_family_file, infer_schema_length=0
        )
        hierarchy_closure = pl.read_csv(
            self._hierarchy_closure_file, infer_schema_length=0
        )
        family = pl.read_csv(self._family_file, infer_schema_length=0)

        # family: id -> name
        for row in family.iter_rows(named=True):
            fam_id = _to_int(row.get("id"))
            name = row.get("name")
            if fam_id is None or not name:
                continue
            self._group_names[fam_id] = name
            self._group_id_by_name[name.lower()] = fam_id

        # hierarchy_closure is already transitive: child_fam_id -> all ancestor ids.
        # include only proper ancestors here (distance > 0); the leaf itself is
        # unioned in separately below.
        ancestors_by_child: dict[int, set[int]] = defaultdict(set)
        for row in hierarchy_closure.iter_rows(named=True):
            child = _to_int(row.get("child_fam_id"))
            parent = _to_int(row.get("parent_fam_id"))
            if child is None or parent is None:
                continue
            distance = _to_int(row.get("distance"))
            if distance == 0 or parent == child:
                continue
            ancestors_by_child[child].add(parent)

        # gene_has_family: hgnc_id -> leaf family ids, expanded to full lineage.
        # The gene-group files use BARE numeric hgnc ids ('3023') while the HGNC
        # complete set (and thus the search index) uses the prefixed 'HGNC:3023'
        # form. Canonicalize here so members resolve against the search index
        # instead of coming back with null symbols/coordinates.
        for row in gene_has_family.iter_rows(named=True):
            hgnc_id = _canonical_hgnc_id(row.get("hgnc_id"))
            leaf = _to_int(row.get("family_id"))
            if not hgnc_id or leaf is None:
                continue
            lineage = self._lineage_by_hgnc_id.setdefault(hgnc_id, set())
            lineage.add(leaf)
            lineage.update(ancestors_by_child.get(leaf, set()))

        # invert to members-by-group for descendant resolution
        for hgnc_id, lineage in self._lineage_by_hgnc_id.items():
            for group_id in lineage:
                self._members_by_group_id[group_id].add(hgnc_id)

        logger.info(
            f"Loaded HGNC gene groups: {len(self._group_names)} groups, "
            f"{len(self._lineage_by_hgnc_id)} genes with lineage"
        )

    def is_loaded(self) -> bool:
        """True if any gene-group lineage data is available."""
        return bool(self._lineage_by_hgnc_id)

    def group_name(self, group_id: int) -> str | None:
        """Return the name of a group id, or None if unknown."""
        return self._group_names.get(group_id)

    def resolve_group_id(self, group_name: str) -> int | None:
        """Resolve a group name (case-insensitive) to its group id."""
        return self._group_id_by_name.get(group_name.lower())

    def group_ids_for_hgnc_id(self, hgnc_id: str) -> set[int]:
        """Full lineage group ids (leaf + all ancestors) for an hgnc_id."""
        return set(self._lineage_by_hgnc_id.get(_canonical_hgnc_id(hgnc_id), set()))

    def groups_for_hgnc_id(self, hgnc_id: str) -> list[tuple[int, str | None]]:
        """Full lineage as (group_id, group_name) pairs for an hgnc_id."""
        hgnc_id = _canonical_hgnc_id(hgnc_id)
        return [
            (gid, self._group_names.get(gid))
            for gid in sorted(self._lineage_by_hgnc_id.get(hgnc_id, set()))
        ]

    def members_of_group(
        self,
        group_id: int | None = None,
        group_name: str | None = None,
        exclude_olfactory: bool = False,
    ) -> set[str]:
        """
        hgnc_ids of all genes whose lineage contains the given group.

        Resolution is by descendant: any group id (leaf or ancestor/root)
        returns every gene below it in the hierarchy. Accepts either a group
        id or a (case-insensitive) group name.

        Olfactory receptors are themselves GPCRs and dominate large families by
        count; set exclude_olfactory=True to drop any member whose lineage
        contains the 'Olfactory receptors' group.
        """
        if group_id is None and group_name is not None:
            group_id = self.resolve_group_id(group_name)
        if group_id is None:
            return set()
        members = set(self._members_by_group_id.get(group_id, set()))
        if exclude_olfactory:
            olfactory_id = self.resolve_group_id(_OLFACTORY_GROUP_NAME)
            if olfactory_id is not None:
                members -= self._members_by_group_id.get(olfactory_id, set())
        return members


# HGNC gene-group name for olfactory receptors; excluded on request because they
# are GPCRs that dominate large families by sheer count.
_OLFACTORY_GROUP_NAME = "Olfactory receptors"


def _canonical_hgnc_id(value) -> str | None:
    """Normalize an HGNC id to its canonical 'HGNC:NNNN' form.

    The HGNC sources disagree on format: hgnc_complete_set.txt (and the search
    index) use the prefixed 'HGNC:3023' form, while the gene-group files
    (hgnc_gene_has_family.csv) use bare numeric ids ('3023'). Normalizing both
    to one key is what lets group members resolve to symbols/coordinates.
    """
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    if v.upper().startswith("HGNC:"):
        return "HGNC:" + v[5:].strip()
    if v.isdigit():
        return f"HGNC:{v}"
    return v


def _to_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None
