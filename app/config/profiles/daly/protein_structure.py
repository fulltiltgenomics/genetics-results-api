"""Daly profile: protein structure layer, no default release pointer.

A daly deployment opts in by setting PROTEIN_STRUCTURE_RELEASE_POINTER (rehearsal:
gs://daly-genetics-results/protein_structure_staging/releases/CURRENT, production:
gs://daly-genetics-results/protein_structure/releases/CURRENT) rather than inheriting one,
so two daly clusters can differ. Unset, no protein structure route is registered.

Every licence is served: daly has cleared the AF3 models' terms.
"""

release_pointer = None
served_licences = None
