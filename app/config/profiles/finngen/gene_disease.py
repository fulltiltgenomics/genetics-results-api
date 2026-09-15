"""FinnGen profile: gene-disease data paths."""

gene_disease = {
    "gencc": {
        "file": "gs://finngen-commons/results_api_data/gene_disease/gencc-submissions-export.2026-09-12.tsv",
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
        "file": "gs://finngen-commons/results_api_data/gene_disease/monarch-gene_to_disease.2026-09-02.tsv",
        "uuid_from": ["subject", "object", "primary_knowledge_source", "predicate"],
        "columns": {
            "uuid": "uuid",
            "gene_symbol": "subject_label",
            "disease_curie": "object",
            "disease_title": "object_label",
            # the Monarch file combines the KG's causal and non-causal gene-disease exports, so
            # the Biolink predicate is what separates "causes" from "gene_associated_with_condition".
            # It lands in the same column as GenCC's validity classification: these columns
            # harmonize per-source vocabularies rather than share one, as `submitter` already does
            # by holding both "Ambry Genetics" and "infores:omim".
            "classification": "predicate",
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
