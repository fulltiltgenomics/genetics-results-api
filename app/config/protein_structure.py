"""
Protein structure layer: configuration.

The profile names only the release POINTER (`releases/CURRENT`, a JSON object locating the
release directory); the contract the release is checked against is
app.config.protein_structure_contract, and the loader is app.services.protein_structure.

The pointer is the per-deployment switch. A profile without a `protein_structure` module
cannot turn the feature on at all, even through the environment: AF3 models are served only
where a deployment has cleared their terms, and a stray env value on another profile must
not publish them.
"""

import os

from app.config.profile import load_profile_module

try:
    _profile = load_profile_module("protein_structure")
except ModuleNotFoundError as e:
    if not (e.name or "").endswith(".protein_structure"):
        raise
    _profile = None

# gs:// URI of the release pointer object, or None where the feature is absent. Empty counts
# as unset so a blank manifest value cannot switch it on.
release_pointer: str | None = (
    (os.environ.get("PROTEIN_STRUCTURE_RELEASE_POINTER") or _profile.release_pointer)
    if _profile is not None
    else None
)

# licences whose models this deployment serves; None serves every licence the release holds.
# An accession with any model outside the set is left out whole, so no protein is served with
# a window missing.
served_licences: frozenset[str] | None = _profile.served_licences if _profile is not None else None
