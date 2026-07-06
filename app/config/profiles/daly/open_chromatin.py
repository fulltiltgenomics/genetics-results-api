"""Daly profile: open-chromatin (Product A) data paths.

One entry per resource that ships an open_chromatin atlas. The files are the
tabix-indexed bgzipped TSVs produced by the munge step and staged to the
daly-genetics-results bucket; versions mirror configs/datasets.yaml.
"""

open_chromatin_data = [
    {
        "data_source": "gcloud",
        "resource": "marderstein",
        "version": "2026",
        "file": "gs://daly-genetics-results/open_chromatin/marderstein/marderstein_open_chromatin.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "li_brain_atac",
        "version": "2023",
        "file": "gs://daly-genetics-results/open_chromatin/li_brain_atac/li_brain_atac_open_chromatin.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "catlas",
        "version": "2021",
        "file": "gs://daly-genetics-results/open_chromatin/catlas/catlas_open_chromatin.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "epimap",
        "version": "2021",
        "file": "gs://daly-genetics-results/open_chromatin/epimap/epimap_open_chromatin.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "calderon_immune",
        "version": "2019",
        "file": "gs://daly-genetics-results/open_chromatin/calderon_immune/calderon_immune_open_chromatin.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "rosmap_brain",
        "version": "2023",
        "file": "gs://daly-genetics-results/open_chromatin/rosmap_brain/rosmap_brain_open_chromatin.tsv.gz",
    },
]
