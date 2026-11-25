"""FinnGen profile: protein structure layer, no default release pointer.

A finngen deployment opts in by setting PROTEIN_STRUCTURE_RELEASE_POINTER
(gs://finngen-commons/results_api_data/protein_structure/releases/CURRENT, a copy of a
release daly built) rather than inheriting one. Unset, no protein structure route is
registered.

Every model licence is served: the AF3 models' terms are cleared for finngen as for daly.

Predictor scores are served under CC-BY-4.0 and CC-BY-NC-4.0, the two licences cleared so
far. A predictor under any other licence is left out until this set is extended.
"""

release_pointer = None
served_licences = None
served_score_licences = frozenset({"CC-BY-4.0", "CC-BY-NC-4.0"})
