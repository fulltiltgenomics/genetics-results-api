"""Daly profile: variant-effect (Product B) data paths.

One entry per DATASET that ships in-silico predicted variant-effect scores. The
Marderstein resource provides two distinct predictor files (ChromBPNet and FLARE),
each a separate per-dataset entry. Files are the tabix-indexed (point-indexed,
-s1 -b2 -e2) bgzipped TSVs produced by the munge step and staged to the
daly-genetics-results bucket; versions mirror configs/datasets.yaml.
"""

variant_effect_data = [
    {
        "data_source": "gcloud",
        "resource": "marderstein",
        "dataset_id": "marderstein_chrombpnet",
        "version": "2026",
        "file": "gs://daly-genetics-results/variant_effect/marderstein/marderstein_chrombpnet.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "marderstein",
        "dataset_id": "marderstein_flare",
        "version": "2026",
        "file": "gs://daly-genetics-results/variant_effect/marderstein/marderstein_flare.tsv.gz",
    },
]
