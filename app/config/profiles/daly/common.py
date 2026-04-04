"""Daly profile: common data paths."""

hgnc_file = "gs://daly-genetics-results/mapping_files/hgnc_complete_set.txt"

rsid_db = {
    "file": "gs://daly-genetics-results/gnomad/gnomad.genomes.exomes.v4.0.rsid.tsv.gz",
}

gnomad = {
    "file": "gs://daly-genetics-results/gnomad/gnomad.genomes.exomes.v4.0.sites.v2.tsv.bgz",
    "populations": ["afr", "amr", "asj", "eas", "fin", "mid", "nfe", "oth", "sas"],
    "url": "https://gnomad.broadinstitute.org/variant/[VARIANT]?dataset=gnomad_r4",
    "version": "4.0",
}

dataset_to_resource = {
    "FinnGen_ATACseq": ("finngen", "R12"),
    "FinnGen_snRNAseq": ("finngen", "R12"),
    "FinnGen_Olink": ("finngen", "batch1_4"),
    "FinnGen_SomaScan": ("finngen", "2023-03-02"),
    "FinnGen_R13": ("finngen", "R13"),
    "FinnGen_R13_MVP_UKBB": ("finngen_mvp_ukbb", "R13"),
    "FinnGen_R13_MVP_UKBB_labs": ("finngen_mvp_ukbb", "R13"),
    "FinnGen_R13_UKBB": ("finngen_ukbb", "R13"),
    "FinnGen_R13_UKBB_labs": ("finngen_ukbb", "R13"),
    "FinnGen_R12": ("finngen", "R12"),
    "FinnGen_kanta": ("finngen", "R12"),
    "FinnGen_drugs": ("finngen", "R12"),
    "FinnGen_NMR": ("finngen_nmr", "1"),
    "FinnLiver": ("finnliver", "1"),
    "GeneRisk": ("generisk", "1"),
    "INTERVAL": ("interval", "1"),
    "UKB_PPP": ("ukbb", "3k"),
    "UKB_Finucane": ("ukbb", "1"),
    "Open_Targets_25.12": ("open_targets", "25.12"),
    "GTEx_v10": ("gtex", "v10"),
    "HPA_24.1": ("hpa", "24.1"),
    "genebass": ("genebass", "v1"),
}

dataset_mapping_files = [
    (
        "gs://daly-genetics-results/mapping_files/eqtl_catalogue_r7_dataset_metadata.tsv",
        "dataset_id",
        "eqtl_catalogue",
        "R7",
    ),
]

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

# phenotype_reports directory does not exist in this bucket yet
phenotype_markdown_template = ""

cors_origins = [
    "https://anno.finngen.fi",
    "https://annopublic.finngen.fi",
    "https://finngenie.finngen.fi",
    "https://finngenie.fi",
]
