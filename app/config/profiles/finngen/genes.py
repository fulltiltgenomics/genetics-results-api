"""FinnGen profile: gene data paths."""

genes = {
    "gencode_versions": [49, 45, 43, 39, 35, 32, 19],
    "gene_name_mapping_file": "gs://finngen-commons/results_api_data/mapping_files/gencode_gene_name_mapping_49-45-43-39-35-32-19.tsv",
    "gene_position_file_template": "gs://finngen-commons/results_api_data/mapping_files/gencode.v{version}.annotation.genes.tsv",
    # EMPTY UNTIL THE FILE IS UPLOADED. See the daly profile for what it is and why only the
    # newest release gets one. Producing it is one command —
    #     python3 gencode_to_exon_tsv_gff.py \
    #       https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gff3.gz \
    #       gencode.v49.annotation.exons.tsv
    # (genetics-results-munge), then keep the `is_canonical` rows, sort by chrom and start,
    # bgzip and tabix `-s 3 -b 4 -e 5 -S 1`, and upload as
    # gencode.v49.annotation.exons.canonical.tsv.gz beside the gene position TSVs. Add the
    # entry here in the same change. Left empty rather than pointed at a missing object
    # because startup_checks existence-checks every configured path and would fail a FinnGen
    # deployment's startup over a file nobody here can upload; empty simply serves genes with
    # no exon arrays, which is what every other GENCODE version does.
    "exon_file_by_version": {},
    "hgnc_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_complete_set.txt",
    "gene_has_family_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_gene_has_family.csv",
    "hierarchy_closure_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_hierarchy_closure.csv",
    "family_file": "gs://finngen-commons/results_api_data/mapping_files/hgnc_family.csv",
}
