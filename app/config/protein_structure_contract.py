"""The slice of the protein structure release contract that results-api reads.

A copy, not an import: the definition lives in genetics-results-munge
`scripts/protein_structure_contract.py`, and no repo of the suite depends on another's code.
Only what the loader checks is copied: the contract version, the release's file names, the
model file id charset and `models.tsv.gz`'s column list. tests/test_protein_structure.py pins
these and compares them with the munge module when a checkout of it is found.
"""

import re

CONTRACT_VERSION = 1

MANIFEST_FILE = "manifest.json"
MODELS_FILE = "models.tsv.gz"
RESIDUES_FILE = "residues.tsv.gz"
FEATURES_FILE = "features.tsv.gz"
VARIANTS_FILE = "variants.tsv.gz"
STRUCTURE_DIR = "structures"
PAE_DIR = "pae"

# model file ids appear in URLs and object paths, so an id outside this is refused at load
MODEL_FILE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def structure_path(mid: str) -> str:
    return f"{STRUCTURE_DIR}/{mid}.pdb.gz"


def pae_path(mid: str) -> str:
    return f"{PAE_DIR}/{mid}.u8"


def header_line(columns: list[tuple[str, type]]) -> str:
    return "#" + "\t".join(c for c, _ in columns) + "\n"


MODEL_COLUMNS = [
    ("model_file_id", str),
    ("accession", str),
    ("uniprot_isoform", str),
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
