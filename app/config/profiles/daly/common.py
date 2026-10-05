"""Daly profile: common data paths."""

hgnc_file = "gs://daly-genetics-results/mapping_files/hgnc_complete_set.txt"

rsid_db = {
    "file": "gs://daly-genetics-results/gnomad/gnomad.genomes.exomes.v4.0.rsid.v2.tsv.gz",
}

gnomad = {
    "file": "gs://daly-genetics-results/gnomad/gnomad.genomes.exomes.v4.0.sites.v2.tsv.bgz",
    "populations": ["afr", "amr", "asj", "eas", "fin", "mid", "nfe", "oth", "sas"],
    "url": "https://gnomad.broadinstitute.org/variant/[VARIANT]?dataset=gnomad_r4",
    "version": "4.0",
}

dataset_mapping_files = [
    (
        "gs://daly-genetics-results/mapping_files/eqtl_catalogue_r8_dataset_metadata.tsv",
        "dataset_id",
        "eqtl_catalogue",
        "R8",
    ),
]

# display-name overrides keyed by the raw `dataset` column value carried in the
# source data files (same key space as dataset_to_resource). the frontend
# humanizes unknown datasets by replacing underscores with spaces; entries here
# override that where the humanized form is wrong or incomplete. UKB_PPP -> "UKB
# PPP" hides that it is only the Olink 3K (Explore 3072) panel, which is why some
# proteins have a FinnGen pQTL but no UKBB one (5K vs 3K coverage).
dataset_display_names = {
    "UKB_PPP": "UKBB PPP (Olink 3K)",
}

variant_set_files = {
    "FinnGen_enriched_202505": {
        "file": "gs://daly-genetics-results/variant_sets/FinnGen_enriched_202505",
    },
    "COVID19_HGI_all": {
        "file": "gs://daly-genetics-results/variant_sets/COVID19_HGI_all",
    },
    "COVID19_HGI_severity": {
        "file": "gs://daly-genetics-results/variant_sets/COVID19_HGI_severity",
    },
}

variant_annotation_sources = {
    "finngen": {
        "file": "gs://daly-genetics-results/variant_annotations/R14_annotated_variants_v0.small.gz",
        "version": "R14",
    },
    "gnomad": {
        "file": "gs://daly-genetics-results/gnomad/gnomad.genomes.exomes.v4.1.1.sites.vep115.tsv.bgz",
        "version": "4.1.1",
        # gnomad cpra layout differs from finngen: chr=0,pos=1,ref=2,alt=3
        "cpra_cols": [0, 1, 2, 3],
    },
}

# phenotype_reports directory does not exist in this bucket yet
phenotype_markdown_template = ""

cors_origins = [
    "https://anno.finngen.fi",
    "https://annopublic.finngen.fi",
    "https://finngenie.finngen.fi",
    "https://finngenie.fi",
]
