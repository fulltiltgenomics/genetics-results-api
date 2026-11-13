"""The slice of the protein structure release contract that results-api reads.

A copy, not an import: the definition lives in genetics-results-munge
`scripts/protein_structure_contract.py`, and no repo of the suite depends on another's code.
Only what the loader checks and the routes serve is copied; the feature vocabularies, the
window rules and the view-spec are not read here.
tests/test_protein_structure.py compares every public name of this module with the munge
module when a checkout of it is found.
"""

import re

CONTRACT_VERSION = 1

MANIFEST_FILE = "manifest.json"
MODELS_FILE = "models.tsv.gz"
RESIDUES_FILE = "residues.tsv.gz"
FEATURES_FILE = "features.tsv.gz"
VARIANTS_FILE = "variants.tsv.gz"
SCORES_FILE = "scores.tsv.gz"
STRUCTURE_DIR = "structures"
PAE_DIR = "pae"

# the GENCODE version whose coordinates the gene-keyed tabix files are indexed on
GENCODE_VERSION = 49

# model file ids appear in URLs and object paths, so an id outside this is refused at load
MODEL_FILE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


def structure_path(mid: str) -> str:
    return f"{STRUCTURE_DIR}/{mid}.pdb.gz"


def pae_path(mid: str) -> str:
    return f"{PAE_DIR}/{mid}.u8"


def header_line(columns: list[tuple[str, type]]) -> str:
    return "#" + "\t".join(c for c, _ in columns) + "\n"


MANIFEST_KEYS = ("contract_version", "release", "built_at", "bundle", "gencode", "lab_inputs",
                 "feature_sources", "score_sources", "counts", "drops", "objects", "directories")
# `objects` maps a release-relative path to {OBJECT_BYTES, OBJECT_SHA256}, one entry per name in
# manifest_object_names(); structures and PAE objects are sized per row in models.tsv.gz instead
MANIFEST_OBJECTS = "objects"
MANIFEST_DIRECTORIES = "directories"
OBJECT_BYTES = "bytes"
OBJECT_SHA256 = "sha256"
DIRECTORY_OBJECTS = "objects"
TABIX_FILES = (RESIDUES_FILE, FEATURES_FILE, VARIANTS_FILE, SCORES_FILE)


def manifest_object_names() -> list[str]:
    return [MODELS_FILE] + [n for f in TABIX_FILES for n in (f, f + ".tbi")]


# (column, type) in file order. Types: str, int, float, bool (written true/false); empty = null
MODEL_COLUMNS = [
    ("model_file_id", str),
    ("accession", str),
    ("uniprot_isoform", str),
    # an accession under several genes has its ids comma-joined, and its names in the same order
    ("gene_id", str),
    ("gene_name", str),
    ("source_gene_symbol", str),
    ("source", str),
    ("source_label", str),
    ("model_version", str),
    ("licence", str),
    ("window_index", int),
    ("n_windows", int),
    ("protein_start", int),
    ("protein_end", int),
    ("n_residues", int),
    ("owned_start", int),
    ("owned_end", int),
    ("has_pae", bool),
    ("structure_path", str),
    ("structure_sha256", str),
    ("structure_bytes", int),
    ("pae_path", str),
    ("pae_sha256", str),
    ("pae_bytes", int),
    ("source_pdb_sha256", str),
    ("source_pae_sha256", str),
]


def split_model_genes(gene_id: str, gene_name: str) -> list[tuple[str, str]]:
    """the (gene_id, gene_name) pairs of one models.tsv row; refuses lists of different lengths."""
    if not gene_id:
        if gene_name:
            raise ValueError("gene_name without gene_id")
        return []
    ids, names = gene_id.split(","), gene_name.split(",")
    if len(ids) != len(names):
        raise ValueError(f"{len(ids)} gene ids but {len(names)} gene names")
    return list(zip(ids, names))

_GENE_ANCHOR = [("gene_id", str), ("chrom", int), ("gene_start", int), ("gene_end", int), ("gene_name", str)]

RESIDUE_COLUMNS = _GENE_ANCHOR + [
    ("accession", str),
    ("model_file_id", str),
    ("file_position", int),
    ("protein_position", int),
    ("residue", str),
    ("is_owner", bool),
    ("ca_x", float),
    ("ca_y", float),
    ("ca_z", float),
    ("plddt", float),
    ("mu", float),
    ("n_with_mu", int),
    ("n_possible", int),
]

FEATURE_COLUMNS = _GENE_ANCHOR + [
    ("accession", str),
    ("source", str),
    ("feature_type", str),
    ("start", int),
    ("end", int),
    ("description", str),
    ("evidence", str),
    ("evidence_tier", str),
]

# one row per (variant, accession); `variant` is chr:pos:ref:alt with the numeric chrom
VARIANT_COLUMNS = [
    ("chrom", int),
    ("pos", int),
    ("ref", str),
    ("alt", str),
    ("variant", str),
    ("accession", str),
    ("gene_id", str),
    ("gene_name", str),
    ("aa_pos", int),
    ("aa_ref", str),
    ("aa_alt", str),
    ("model_file_id", str),
    ("file_position", int),
    ("mu_snp", float),
]

# which end of a predictor's raw score is the damaging one. `percentile` ranks the raw mean
# ascending for every predictor, so it is read through this and never as "high = damaging"
SCORE_HIGHER_IS_DAMAGING = "higher_is_more_damaging"
SCORE_LOWER_IS_DAMAGING = "lower_is_more_damaging"
SCORE_DIRECTIONS = {
    "alphamissense": SCORE_HIGHER_IS_DAMAGING,
    "esm1b": SCORE_LOWER_IS_DAMAGING,
}

# long: one row per (gene_id, accession, protein_position, predictor), never per model window.
# No row means the source has no score for that residue
SCORE_COLUMNS = _GENE_ANCHOR + [
    ("accession", str),
    ("predictor", str),
    ("protein_position", int),
    ("aa_ref", str),
    ("mean_snv", float),
    ("n_snv", int),
    ("percentile", float),
    ("mean_all", float),
    ("n_all", int),
]

# PAE: one row-major n x n uint8 object per model file, value = code / PAE_SCALE
PAE_SCALE = 8
PAE_NO_VALUE = 255

# SDK limit on one HTTP response
MAX_RESPONSE_BYTES = 16 * 1024 * 1024


def pae_rows_per_request(n: int) -> int:
    """the most rows of an n x n PAE object one request may ask for: floor(MAX_RESPONSE_BYTES / n)."""
    if n < 1:
        raise ValueError("n must be positive")
    return MAX_RESPONSE_BYTES // n


# `<UTC date>-<HHMMSS>` of the build; it names a directory and travels in an HTTP header, so
# nothing else may pass. `[0-9]`, not `\d`, which also matches every other script's digits
RELEASE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{6}\Z")
