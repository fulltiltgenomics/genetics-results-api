authentication = True
authentication_file = "/mnt/disks/dist_data/finngen_auth_dev.json"

metadata_db = "/mnt/disks/dist_data/meta_finngen_version_20250526.db"

rsid_db = {
    "file": "/mnt/disks/dist_data/gnomad/gnomad.genomes.exomes.v4.0.rsid.db",
}

gnomad = {
    "file": "/mnt/disks/dist_data/gnomad/gnomad.genomes.exomes.v4.0.sites.v2.tsv.bgz",
    "populations": ["afr", "amr", "asj", "eas", "fin", "mid", "nfe", "oth", "sas"],
    "url": "https://gnomad.broadinstitute.org/variant/[VARIANT]?dataset=gnomad_r4",
    "version": "4.0",
}

genes = {
    "model_file": "/mnt/disks/dist_data/ensembl_anno_coding_canonical_105.tsv.gz",
    "start_end_file": "/mnt/disks/dist_data/ensembl_gene_pos_coding_canonical_105.tsv",
}

variant_set_files = {
    "FinnGen_enriched_202505": {
        "file": "/mnt/disks/dist_data/variant_sets/FinnGen_enriched_202505",
    },
    "COVID19_HGI_all": {
        "file": "/mnt/disks/dist_data/variant_sets/COVID19_HGI_all",
    },
    "COVID19_HGI_severity": {
        "file": "/mnt/disks/dist_data/variant_sets/COVID19_HGI_severity",
    },
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

ld_assoc = {
    "file": "/mnt/disks/dist_data/ot_v2d_no_finngen_22.09_with_studycols.tsv.gz",
    "resources": [
        {
            "resource": "Open_Targets",
            "version": "October 2022",
            "data_types": ["GWAS"],
        }
    ],
}

assoc_files = [
    {
        "file": "/mnt/disks/dist_data/FinnGen_MVP_UKBB_meta_sumstats_p0.005.tsv.gz",
        "resource": "FinnGen_MVP_UKBB_meta",
        "version": "R12",
        "data_types": ["GWAS"],
        "n_traits": "330",
        "ignore_phenos": ["G6_SLEEPAPNO_INCLAVO", "K11_CARIES_1_OPER_ONLYAVO"],
        "url": "https://mvp-ukbb.finngen.fi/about",
        "pheno_urls": [
            {
                "url": "https://https://mvp-ukbb.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen/MVP/UKBB meta Pheweb",
            },
            {
                "url": "https://risteys.finngen.fi/endpoints/[PHENOCODE]",
                "label": "FinnGen Risteys",
            },
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_R12_UKBB_meta_sumstats_p0.005.tsv.gz",
        "resource": "FinnGen_UKBB_meta",
        "version": "R12",
        "data_types": ["GWAS"],
        "n_traits": "873",
        "url": "https://metaresults-ukbb.finngen.fi/about",
        "pheno_urls": [
            {
                "url": "https://metaresults-ukbb.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen/UKBB meta Pheweb",
            },
            {
                "url": "https://risteys.finngen.fi/endpoints/[PHENOCODE]",
                "label": "FinnGen Risteys",
            },
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_R12_sumstats_p0.005.tsv.gz",
        "resource": "FinnGen",
        "version": "R12",
        "data_types": ["GWAS"],
        "n_traits": "2,469",
        "url": "https://finngen.gitbook.io/documentation/methods/phewas",
        "pheno_urls": [
            {
                "url": "https://r12.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen Pheweb",
            },
            {
                "url": "https://risteys.finngen.fi/endpoints/[PHENOCODE]",
                "label": "FinnGen Risteys",
            },
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_kanta_sumstats_20250315_p0.005.tsv.gz",
        "resource": "FinnGen_kanta",
        "version": "R12",
        "data_types": ["GWAS"],
        "n_traits": "382",
        "url": "https://www.finngen.fi/en/for_researchers",
        "pheno_urls": [
            {
                "url": "https://kanta.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen Pheweb",
            },
            {
                "url": "https://risteys.finngen.fi/lab-tests/[PHENOCODE]",
                "label": "FinnGen Risteys",
            },
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_R12drugs_sumstats_p0.005.tsv.gz",
        "resource": "FinnGen_drugs",
        "version": "R12",
        "data_types": ["GWAS"],
        "n_traits": "126",
        "ignore_phenos": ["ATC_D01_IRN"],
        "url": "https://www.finngen.fi/en/for_researchers",
        "pheno_urls": [
            {
                "url": "https://drugs.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen Pheweb",
            },
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_Olink_meta_sumstats_2025-03-20_SomaScan_2023-03-02_sumstats_p0.005.tsv.gz",
        "resource": "FinnGen_pQTL",
        "version": "2025-03-20",
        "data_types": ["pQTL"],
        "n_traits": "2,826 Olink, 7,596 SomaScan",
        "url": "https://finngen.gitbook.io/documentation/methods/pqtl-analysis",
        "pheno_urls": [
            {
                "url": "https://r12.finngen.fi/gene/[GENE]",
                "label": "FinnGen Pheweb",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_snRNAseq_2023-10-05_sumstats.tsv.gz",
        "resource": "FinnGen_eQTL",
        "version": "2023-10-05",
        "data_types": ["eQTL"],
        "n_traits": "26,624 genes, 35 predicted cell types",
        "url": "https://www.finngen.fi/en/for_researchers",
        "pheno_urls": [
            {
                "url": "https://r12.finngen.fi/gene/[GENE]",
                "label": "FinnGen Pheweb",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/UKBB_Olink_sumstats_2024-04-12_p0.005.tsv.gz",
        "resource": "UKBB_pQTL",
        "version": "2024-04-12",
        "data_types": ["pQTL"],
        "n_traits": "2,655",
        # "url": "https://www.biorxiv.org/content/10.1101/2022.06.17.496443v1.full",
        "url": None,
        "pheno_urls": [
            {
                "url": "https://www.finngen.fi/en/for_researchers",
                "label": "FinnGen analysis",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/ot_sa_gwas_no_finngen_22.09.tsv.gz",
        "resource": "Open_Targets",
        "version": "October 2022 (associations), March 2025 (credible sets)",
        "data_types": ["GWAS"],
        "n_traits": "6,033 (associations), 14,350 (credible sets)",
        "url": "https://platform.opentargets.org",
        "pheno_urls": [
            {
                "url": "https://platform.opentargets.org/study/[PHENOCODE]",
                "label": "Open Targets study",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/eQTL_Catalogue_R7_sumstats_p0.005.tsv.gz",
        "resource": "eQTL_Catalogue_R7",
        "version": "R7",
        "data_types": ["eQTL", "pQTL", "sQTL"],
        "n_traits": "1,000,000+",
        "url": "https://www.ebi.ac.uk/eqtl/",
        "pheno_urls": [
            {
                "url": "https://www.ebi.ac.uk/eqtl/Studies",
                "label": "eQTL Catalogue studies",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/deCODE_pQTLs_NatGen2021_aligned_p0.005.tsv.gz",
        "resource": "deCODE",
        "version": "2021",
        "data_types": ["pQTL"],
        "n_traits": "4,907",
        "url": "https://doi.org/10.1038/s41588-021-00978-w",
        "pheno_urls": [
            {
                "url": "https://doi.org/10.1038/s41588-021-00978-w",
                "label": "Nat Genet 2021",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/GTEx_v8_edQTL.tsv.gz",
        "resource": "GTEx_v8_edQTL",
        "version": "2022",
        "data_types": ["edQTL"],
        "n_traits": "156,396",
        "url": "https://doi.org/10.1038/s41586-022-05052-x",
        "pheno_urls": [
            {
                "url": "https://doi.org/10.1038/s41586-022-05052-x",
                "label": "Nature 2022",
            }
        ],
        "p_thres": 1e-6,
    },
    {
        "file": "/mnt/disks/dist_data/NMR_sumstats_p0.005.tsv.gz",
        "resource": "NMR",
        "version": "2024",
        "data_types": ["metaboQTL"],
        "n_traits": "250",
        "url": "https://www.medrxiv.org/content/10.1101/2023.06.09.23291213v1",
        "pheno_urls": [
            {
                "url": "https://www.medrxiv.org/content/10.1101/2023.06.09.23291213v1",
                "label": "medRxiv 2023",
            }
        ],
        "p_thres": 5e-3,
    },
]

# finemapped = {
#     "file": "/mnt/disks/dist_data/finemapped_resources_finngen_version_20250526_eqtlcat_ge_aptamer_genenamesmapped.annotated.tsv.gz",
#     "trans_file": "/mnt/disks/dist_data/finemapped_resources_finngen_version_20250526_eqtlcat_ge_aptamer_genenamesmapped_eqtlpqtl_sortbygenepos.annotated.tsv.gz",
# }

finemapped = {
    "file": "/mnt/disks/dist_data/finemapped_resources_finngen_version_20250315_eqtlcat_ge_aptamer_genenamesmapped.annotated.tsv.gz",
    "trans_file": "/mnt/disks/dist_data/finemapped_resources_finngen_version_20250315_eqtlcat_ge_aptamer_genenamesmapped_eqtlpqtl_sortbygenepos.annotated.tsv.gz",
}

finemapped_files = [
    {
        "file": "/mnt/disks/dist_data/ot_cs_gwas_no_finngen_25.03.tsv.gz",
        "resource": "Open_Targets",
        "version": "October 2022 (associations), March 2025 (credible sets)",
        "data_types": ["GWAS"],
        "n_traits": "6,033 (association results), 14,350 (credible set results)",
        "url": "https://platform.opentargets.org",
        "pheno_urls": [
            {
                "url": "https://platform.opentargets.org/study/[PHENOCODE]",
                "label": "Open Targets study",
            }
        ],
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_R12_credible_sets.tsv.gz",
        "resource": "FinnGen",
        "version": "R12",
        "data_types": ["GWAS"],
        "n_traits": "2,469",
        "url": "https://finngen.gitbook.io/documentation/methods/phewas",
        "pheno_urls": [
            {
                "url": "https://r12.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen Pheweb",
            },
            {
                "url": "https://risteys.finngen.fi/endpoints/[PHENOCODE]",
                "label": "FinnGen Risteys",
            },
        ],
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_kanta_credible_sets_20250315.tsv.gz",
        "resource": "FinnGen_kanta",
        "version": "2025-03-15",
        "data_types": ["GWAS"],
        "n_traits": "382",
        "url": "https://www.finngen.fi/en/for_researchers",
        "pheno_urls": [
            {
                "url": "https://kanta.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen Pheweb",
            },
        ],
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_drugs_credible_sets.tsv.gz",
        "resource": "FinnGen_drugs",
        "version": "2025-03-15",
        "data_types": ["GWAS"],
        "n_traits": "126",
        "ignore_phenos": ["ATC_D01_IRN"],
        "url": "https://www.finngen.fi/en/for_researchers",
        "pheno_urls": [
            {
                "url": "https://drugs.finngen.fi/pheno/[PHENOCODE]",
                "label": "FinnGen Pheweb",
            },
        ],
    },
    {
        "file": "/mnt/disks/dist_data/UKBB_pQTL_2024-04-12_credible_sets.tsv.gz",
        "resource": "UKBB_pQTL",
        "version": "2024-01-30",
        "data_types": ["pQTL"],
        "n_traits": "Olink 2,655",
        # "url": "https://www.biorxiv.org/content/10.1101/2022.06.17.496443v1.full",
        "url": None,
        "pheno_urls": [
            {
                "url": "https://www.finngen.fi/en/for_researchers",
                "label": "FinnGen analysis",
            }
        ],
        "p_thres": 5e-3,
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_snRNAseq_2023-10-05_credible_sets.tsv.gz",
        "resource": "FinnGen_eQTL",
        "version": "2023-10-05",
        "data_types": ["eQTL"],
        "n_traits": "26,624 genes, 35 predicted cell types",
        "url": "https://www.finngen.fi/en/for_researchers",
        "pheno_urls": [
            {
                "url": "https://r12.finngen.fi/gene/[GENE]",
                "label": "FinnGen Pheweb",
            }
        ],
    },
    {
        "file": "/mnt/disks/dist_data/ukbb_119_credible_sets.tsv.gz",
        "resource": "UKBB_119",
        "version": "2021",
        "data_types": ["GWAS"],
        "n_traits": "119",
        "url": "https://www.medrxiv.org/content/10.1101/2021.09.03.21262975v1.full",
    },
    {
        "file": "/mnt/disks/dist_data/bbj_79_credible_sets.tsv.gz",
        "resource": "BBJ_79",
        "version": "2021",
        "data_types": ["GWAS"],
        "n_traits": "79",
        "url": "https://www.medrxiv.org/content/10.1101/2021.09.03.21262975v1.full",
    },
    {
        "file": "/mnt/disks/dist_data/FinnGen_SomaScan_2023-03-02_Olink_meta_2025-03-20_credible_sets.tsv.gz",
        "resource": "FinnGen_pQTL",
        "version": "2025-03-20",
        "data_types": ["pQTL"],
        "n_traits": "2,826 Olink, 7,596 SomaScan",
        "url": "https://finngen.gitbook.io/documentation/methods/pqtl-analysis",
    },
    {
        "file": "/mnt/disks/dist_data/NMR_credible_sets.tsv.gz",
        "resource": "NMR",
        "version": "2024",
        "data_types": ["metaboQTL"],
        "n_traits": "250",
        "url": "https://www.medrxiv.org/content/10.1101/2023.06.09.23291213v1",
    },
    {
        "file": "/mnt/disks/dist_data/eQTL_Catalogue_R7_credible_sets.tsv.gz",
        "resource": "eQTL_Catalogue_R7",
        "version": "R7",
        "data_types": ["eQTL", "pQTL", "sQTL"],
        "n_traits": "1,000,000+",
        "url": "https://www.ebi.ac.uk/eqtl/",
    },
]

max_query_variants = 2000
