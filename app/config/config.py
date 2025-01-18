authentication = False

metadata_db = "/mnt/disks/data/meta_finngen_version_20241124.db"

rsid_db = {
    "file": "/mnt/disks/data/gnomad/gnomad.genomes.exomes.v4.0.rsid.db",
}

gnomad = {
    "file": "/mnt/disks/data/gnomad/gnomad.genomes.exomes.v4.0.sites.v2.tsv.bgz",
    "populations": ["afr", "amr", "asj", "eas", "fin", "mid", "nfe", "oth", "sas"],
    "url": "https://gnomad.broadinstitute.org/variant/[VARIANT]?dataset=gnomad_r4",
    "version": "4.0",
}

genes = {
    "model_file": "/mnt/disks/data/ensembl_anno_canonical.tsv.gz",
    "start_end_file": "/mnt/disks/data/ensembl_gene_pos.tsv",
}

coding_set = set(
    [
        "missense",
        "frameshift",
        "inframe insertion",
        "inframe deletion",
        "transcript ablation",
        "stop gained",
        "stop lost",
        "start lost",
        "splice acceptor",
        "splice donor",
        "incomplete terminal codon",
        "protein altering",
        "coding sequence",
    ]
)

assoc = {
    "file": "/mnt/disks/data/assoc_resources_public_version_20240219.tsv.gz",
}

finemapped = {
    "file": "/mnt/disks/data/finemapped_resources_finngen_version_20241124.tsv.gz",
}

max_query_variants = 2000
