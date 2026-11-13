"""Daly profile: protein structure layer, no default release pointer.

A daly deployment opts in by setting PROTEIN_STRUCTURE_RELEASE_POINTER (rehearsal:
gs://daly-genetics-results/protein_structure_staging/releases/CURRENT, production:
gs://daly-genetics-results/protein_structure/releases/CURRENT) rather than inheriting one,
so two daly clusters can differ. Unset, no protein structure route is registered.

Every model licence is served: daly has cleared the AF3 models' terms.

Predictor scores are served under CC-BY-4.0 and CC-BY-NC-4.0, the two licences cleared so
far. A predictor under any other licence is left out until this set is extended.
"""

release_pointer = None
served_licences = None
served_score_licences = frozenset({"CC-BY-4.0", "CC-BY-NC-4.0"})
