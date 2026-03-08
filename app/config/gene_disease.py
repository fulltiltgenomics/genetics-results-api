"""
Gene-to-disease configuration.

This module contains configuration for gene-to-disease relationship data
from multiple sources that are harmonized at load time.
"""

# cd /mnt/disks/data
# curl -LO https://search.thegencc.org/download/action/submissions-export-tsv
# curl -LO https://data.monarchinitiative.org/monarch-kg/latest/tsv/all_associations/causal_gene_to_disease_association.all.tsv.gz && gunzip causal_gene_to_disease_association.all.tsv.gz

gene_disease = {
    "gencc": {
        "file": "gs://finngen-commons/results_api_data/gene_disease/gencc-submissions-export-tsv",
        "columns": {
            "uuid": "uuid",
            "gene_symbol": "gene_symbol",
            "disease_curie": "disease_curie",
            "disease_title": "disease_title",
            "classification": "classification_title",
            "mode_of_inheritance": "moi_title",
            "submitter": "submitter_title",
        },
    },
    "monarch": {
        "file": "gs://finngen-commons/results_api_data/gene_disease/monarch-causal_gene_to_disease_association.all.tsv",
        "columns": {
            "uuid": "uuid",
            "gene_symbol": "subject_label",
            "disease_curie": "object",
            "disease_title": "object_label",
            "submitter": "primary_knowledge_source",
        },
    },
    "output_columns": [
        "resource",
        "uuid",
        "gene_symbol",
        "disease_curie",
        "disease_title",
        "classification",
        "mode_of_inheritance",
        "submitter",
    ],
}
