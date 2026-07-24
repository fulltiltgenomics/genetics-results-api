"""FinnGen profile: MPRA (Siraj et al.) data paths.

One entry per DATASET that ships MPRA per-variant functional annotation. The Siraj
resource provides a single combined LONG tabix file (one row per variant x cell_line).
The file is the tabix-indexed (point-indexed, -s1 -b2 -e2) bgzipped TSV produced by
the munge step and staged to the finngen-commons bucket; version mirrors
configs/datasets.yaml.
"""

mpra_data = [
    {
        "data_source": "gcloud",
        "resource": "siraj_mpra",
        "dataset_id": "siraj_mpra",
        "version": "2026",
        "file": "gs://finngen-commons/results_api_data/mpra/siraj_mpra/siraj_mpra.tsv.gz",
    },
]
