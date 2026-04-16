"""FinnGen profile: central dataset registry.

See app/config/profiles/daly/datasets.py for field documentation. This profile
mirrors the daly registry but points metadata_file paths to the finngen-commons
GCS bucket.
"""

datasets = {
    "finngen_gwas": {
        "resource": "finngen",
        "version": "R13",
        "description": "FinnGen R13 core GWAS. Binary disease endpoints and height/weight/BMI quantitative traits in 500,186 Finnish biobank participants.",
        "author": "FinnGen Consortium",
        "publication_date": "2025-09-12",
        "trait_type": "binary",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r13_pheno_202509.json",
        "metadata_harmonizer": "finngen_r13",
    },
    "finngen_kanta": {
        "resource": "finngen",
        "version": "2025-03",
        "description": (
            "FinnGen Kanta lab-value GWAS. These GWASs are based on clinical lab values used in "
            "standard medical care throughout Finland (covering public and private health care providers) "
            "and centralised to the KANTA register. "
            "The data from the Kanta register includes all basic laboratory tests used in medical care "
            "as well as a number of less common laboratory results. Laboratory data are available for more "
            "than 400,000 FinnGen participants. "
            "The first Kanta laboratory results are from 2014, with comprehensive results available from 2018 onwards. "
            "Included in the results are all available labs with >=1000 individuals (n=383). "
            "FinnGen's experts carried out extensive quality control of the laboratory values before introducing "
            "the new data into FinnGen. "
            "In addition, the data have been harmonised and adapted to international standards (OMOP) to facilitate "
            "the straightforward use of the data in research. "
            "GWAS phenotypes are the median of lab values for an individual (quantitative labs) or ever positive (binary labs)."
        ),
        "author": "FinnGen Consortium",
        "publication_date": "2025-03-10",
        "trait_type": "mixed",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/Kanta_metadata_202503.tsv",
        "metadata_harmonizer": "finngen_kanta",
    },
    "finngen_drugs": {
        "resource": "finngen",
        "version": "R12",
        "description": (
            "FinnGen R12 drug-purchase GWAS. These summary stats have been generated with the average "
            "number of drug purchases (ATC codes) as phenotypes. For each ATC code, the quantitative "
            "phenotype is the average number of purchases per year of follow-up with zero purchases "
            "included as 0.5 purchases, followed by rank-based inverse normal transformation. "
            "459,554 individuals were included in the analysis. We required 15 years of drug registry "
            "data (born before 1996 and alive in 2010) for individuals to be included in the analysis. "
            "ATC codes with more than 500,000 purchases in total in the dataset were chosen as basis "
            "of the list of phenotypes to analyze. Nearly identical ATC codes (> 90% overlap) were removed, "
            "and the resulting list was manually expert-curated to combine some ATC codes and add some with "
            "slightly less than 500,000 purchases. The GWAS analysis was done in the same way as FinnGen R12 "
            "core analysis for quantitative phenotypes, except age^2 and age*sex were included as additional "
            "covariates. Each file is named by the analyzed ATC code, or combination of codes. "
            "ATC_TOTAL_IRN is all ATC codes combined or the average number of all drug purchases per year. "
            "ATC_NON_J01_TOTAL_IRN is all ATC codes except J01 (antibacterials). The results have been "
            "filtered by AF 0.001 and INFO score 0.6 to remove rare variant false positives."
        ),
        "author": "FinnGen Consortium",
        "publication_date": "2024-10-01",
        "trait_type": "quantitative",
        "data_type": "gwas",
        "n_samples": 459553,
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r12_drugs_pheno.json",
        "metadata_harmonizer": "finngen_drugs",
    },
    "finngen_mvp_ukbb": {
        "resource": "finngen_mvp_ukbb",
        "version": "R13",
        "description": (
            "FinnGen R13 + MVP + UK Biobank meta-analysis (disease endpoints). "
            "These genetic association results represent a multi-way meta analysis "
            "(using standard inverse variance weighting). "
            "This current release contains aligned disease endpoints/phecodes from "
            "VA Million Veteran Program (3 analyses: n EUR=449,042; n AFR=121,177; n AMR=59,048) "
            "FinnGen freeze 13 (n=500,186) UKBB (pan-UKBB European subset n=420,531). "
            "For these data there are pseudo credible sets, no formal fine-mapping results."
        ),
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-19",
        "trait_type": "mixed",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r13_mvp_ukbb_nonlabs.json",
        "metadata_harmonizer": "finngen_r13",
        "pseudo_credible_sets": True,
    },
    "finngen_mvp_ukbb_labs": {
        "resource": "finngen_mvp_ukbb",
        "version": "R13",
        "description": (
            "FinnGen R13 + MVP + UK Biobank meta-analysis (lab measurements). "
            "These genetic association results represent a multi-way meta analysis "
            "(using standard inverse variance weighting). "
            "This current release contains aligned lab measurements from "
            "VA Million Veteran Program (3 analyses: n EUR=449,042; n AFR=121,177; n AMR=59,048) "
            "FinnGen freeze 13 (n=500,186) UKBB (pan-UKBB European subset n=420,531). "
            "For these data there are pseudo credible sets, no formal fine-mapping results."
        ),
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-19",
        "trait_type": "quantitative",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r13_mvp_ukbb_labs.json",
        "metadata_harmonizer": "finngen_r13",
        "pseudo_credible_sets": True,
    },
    "finngen_ukbb": {
        "resource": "finngen_ukbb",
        "version": "R13",
        "description": (
            "FinnGen R13 + UK Biobank meta-analysis (disease endpoints). "
            "These genetic association results are from the FinnGen study meta-analyzed "
            "(inverse variance weighting) with 936 matching disease endpoints "
            "(matching based on ICD-10 definitions of endpoints) from the pan-UKBB study "
            "(https://pan.ukbb.broadinstitute.org/). "
            "European subset of the pan ukbb study was used. "
            "Some of the pan-ukbb endpoints were custom defined to better match the FinnGen definitions. "
            "The FinnGen results are based on data freeze 13, consisting of 500,186 Finnish individuals."
        ),
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-19",
        "trait_type": "mixed",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r13_ukbb_nonlabs.json",
        "metadata_harmonizer": "finngen_r13",
        "pseudo_credible_sets": True,
    },
    "finngen_ukbb_labs": {
        "resource": "finngen_ukbb",
        "version": "R13",
        "description": (
            "FinnGen R13 + UK Biobank meta-analysis (disease endpoints). "
            "These genetic association results are from the FinnGen study meta-analyzed "
            "(inverse variance weighting) with 48 lab measurements from the pan-UKBB study "
            "(https://pan.ukbb.broadinstitute.org/). "
            "European subset of the pan ukbb study was used. "
            "Some of the pan-ukbb endpoints were custom defined to better match the FinnGen definitions. "
            "The FinnGen results are based on data freeze 13, consisting of 500,186 Finnish individuals."
        ),
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-19",
        "trait_type": "quantitative",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r13_ukbb_labs.json",
        "metadata_harmonizer": "finngen_r13",
        "pseudo_credible_sets": True,
    },
    "finngen_pqtl": {
        "resource": "finngen",
        "version": "batch1-4",
        "description": "FinnGen Olink pQTL. Plasma protein levels measured by Olink Explore 3072 (batches 1-3) and Olink Explore HT (batch 4). Unrelated samples were used in analysis. Sample size is 3,561. The samples were collected from blood donors who were heterozygous or homozygous carriers of specific genetic variants of interest.",
        "author": "FinnGen Consortium",
        "publication_date": "2025-01-01",
        "trait_type": "quantitative",
        "data_type": "pqtl",
        "n_samples": 3561,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "ukbb_pqtl": {
        "resource": "ukbb",
        "version": "PPP",
        "description": "UK Biobank Pharma Proteomics Project (UKB-PPP) pQTL. Plasma protein levels measured by Olink Explore 3072. Sample size is 42,853.",
        "author": "UKB-PPP (FinnGen analysis)",
        "publication_date": "2024-04-12",
        "trait_type": "quantitative",
        "data_type": "pqtl",
        "n_samples": 42853,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "finngen_eqtl": {
        "resource": "finngen",
        "version": "batch1-5",
        "description": "FinnGen single-cell RNA-seq eQTL. Cell-type-resolved expression QTLs from snRNAseq batches 1-5. Sample size is 1,108. Available predicted cell types: l1.B, l1.CD4_T, l1.CD8_T, l1.DC, l1.Mono, l1.NK, l1.PBMC, l1.other, l1.other_T, l2.B_intermediate, l2.B_memory, l2.B_naive, l2.CD14_Mono, l2.CD16_Mono, l2.CD4_CTL, l2.CD4_Naive, l2.CD4_TCM, l2.CD4_TEM, l2.CD8_Naive, l2.CD8_TEM, l2.HSPC, l2.ILC, l2.MAIT, l2.NK, l2.NK_CD56bright, l2.Plasmablast, l2.Platelet, l2.Treg, l2.cDC2, l2.dnT, l2.gdT, l2.pDC. Cell types were predicted based on GEX using the reference PBMC dataset from Hao et al. (2020) (http://dx.doi.org/10.1016/j.cell.2021.04.048). Citation: Kanai, M. et al.: Population-scale multiome immune cell atlas reveals complex disease drivers (https://doi.org/10.1101/2025.11.25.25340489).",
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-15",
        "trait_type": "quantitative",
        "data_type": "eqtl",
        "n_samples": 1108,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "finngen_caqtl": {
        "resource": "finngen",
        "version": "batch1-5",
        "description": "FinnGen ATAC-seq caQTL. Cell-type-resolved chromatin accessibility QTLs from snRNAseq batches 1-5. Sample size is 1,108. Available predicted cell types: l1.B, l1.CD4_T, l1.CD8_T, l1.DC, l1.Mono, l1.NK, l1.PBMC, l1.other, l1.other_T, l2.B_intermediate, l2.B_memory, l2.B_naive, l2.CD14_Mono, l2.CD16_Mono, l2.CD4_CTL, l2.CD4_Naive, l2.CD4_TCM, l2.CD4_TEM, l2.CD8_Naive, l2.CD8_TEM, l2.HSPC, l2.ILC, l2.MAIT, l2.NK, l2.NK_CD56bright, l2.Plasmablast, l2.Platelet, l2.Treg, l2.cDC2, l2.dnT, l2.gdT, l2.pDC. Cell types were predicted based on GEX using the reference PBMC dataset from Hao et al. (2020) (http://dx.doi.org/10.1016/j.cell.2021.04.048). Citation: Kanai, M. et al.: Population-scale multiome immune cell atlas reveals complex disease drivers (https://doi.org/10.1101/2025.11.25.25340489).",
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-15",
        "trait_type": "quantitative",
        "data_type": "caqtl",
        "n_samples": 1108,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "eqtl_catalogue": {
        "resource": "eqtl_catalogue",
        "version": "R7",
        "description": "eQTL Catalogue R7. A harmonized collection of eQTL / sQTL / pQTL studies across many tissues, cell types, and conditions. Each sub-study is identified by a QTD identifier. eQTL results contain gene-level, transcript-level, exon-level and txrevise results. Full summary stats are not available for the eQTL Catalogue.",
        "author": "eQTL Catalogue",
        "publication_date": "2024-06-01",
        "trait_type": "quantitative",
        "data_type": "mixed",
        "qtl_types": ["eQTL", "sQTL", "pQTL"],
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/eqtl_catalogue_r7_dataset_metadata.tsv",
        "metadata_harmonizer": "eqtl_catalogue",
        "collection": True,
        "subdataset_id_field": "phenotype_code",
    },
    "open_targets": {
        "resource": "open_targets",
        "version": "25.12",
        "description": "Open Targets Genetics Portal (release 25.12). GWAS studies from the GWAS Catalog, Million Veterans Program and more with uniform fine-mapping. We include only non-FinnGen SuSiE fine-mapped results from Open Targets.",
        "author": "Open Targets",
        "publication_date": "2025-12-10",
        "trait_type": "mixed",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/credible_sets/open_targets/202512/ot_2512_data_studies.json",
        "metadata_harmonizer": "open_targets",
    },
    # coloc-only datasets: different version or external datasets used as colocalization partners
    "finngen_gwas_r12": {
        "resource": "finngen",
        "version": "R12",
        "description": "FinnGen R12 core GWAS. Used as colocalization partner for certain datasets; R13 is the current release for other analyses.",
        "author": "FinnGen Consortium",
        "publication_date": "2024-05-24",
        "trait_type": "binary",
        "data_type": "gwas",
        "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/finngen_r12_pheno.json",
        "metadata_harmonizer": "finngen_r13",
    },
    "finngen_nmr": {
        "resource": "finngen",
        "version": "NA",
        "description": "Nightingale nuclear magnetic resonance (NMR) metabolomics QTLs in plasma. 250 metabolites in 34,218 samples from THL biobank.",
        "author": "FinnGen Consortium",
        "publication_date": "2025-03-13",
        "trait_type": "quantitative",
        "data_type": "metaboqtl",
        "n_samples": 34218,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "finngen_somascan": {
        "resource": "finngen",
        "version": "batch1-2",
        "description": "FinnGen SomaScan proteomics pQTL. Plasma protein levels measured by the SomaScan 7K platform. Unrelated samples were used in analysis. Sample size is 1,000.",
        "author": "FinnGen Consortium",
        "publication_date": "2023-10-11",
        "trait_type": "quantitative",
        "data_type": "pqtl",
        "n_samples": 1000,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "finnliver": {
        "resource": "finnliver",
        "version": "NA",
        "description": "FinnLiver liver tissue eQTL/sQTL. 131 Finnish individuals.",
        "author": "FinnLiver",
        "publication_date": "2022-07-01",
        "trait_type": "quantitative",
        "data_type": "mixed",
        "qtl_types": ["eQTL", "sQTL"],
        "n_samples": 131,
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "generisk": {
        "resource": "generisk",
        "version": "NA",
        "description": "GeneRisk lipid metabolome GWAS. 186 lipid species QTLs, SuSiE fine-mapping of Widen et al. (2020), 7,632 Finnish samples",
        "author": "GeneRisk",
        "publication_date": "2022-02-07",
        "trait_type": "mixed",
        "data_type": "gwas",
        "n_samples": 7632,
        "n_phenotypes": 186,
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "interval": {
        "resource": "interval",
        "version": "NA",
        "description": "INTERVAL study pQTL. 3,622 plasma proteins in 3,301 samples on a SomaScan assay. Publication: https://pmc.ncbi.nlm.nih.gov/articles/PMC6697541/",
        "author": "INTERVAL",
        "publication_date": "2018-06-06",
        "trait_type": "quantitative",
        "data_type": "pqtl",
        "n_samples": 3301,

        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "ukbb_finucane": {
        "resource": "ukbb_finucane",
        "version": "NA",
        "description": "UK Biobank GWAS of 119 traits. Publication: https://www.medrxiv.org/content/10.1101/2021.09.03.21262975v1",
        "author": "Masahiro Kanai",
        "publication_date": "2021-09-05",
        "trait_type": "mixed",
        "data_type": "gwas",
        "n_samples": 361194,
        "n_phenotypes": 119,
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    # exome / gene-based results
    "genebass_exome": {
        "resource": "genebass",
        "version": "NA",
        "description": (
            "GeneBass exome sequencing variant-level results. Rare variant association testing "
            "from exome sequencing of 394,841 UK Biobank participants. Results include "
            "per-variant burden test statistics across phenotypes and functional annotations."
        ),
        "author": "Genebass",
        "publication_date": "2022-09-14",
        "trait_type": "mixed",
        "data_type": "exome",
        "n_samples": 394841,
        "metadata_file": "gs://daly-genetics-results/mapping_files/genebass_pheno_results.txt.bgz",
        "metadata_harmonizer": "genebass",
    },
    "genebass_gene_based": {
        "resource": "genebass",
        "version": "NA",
        "description": (
            "GeneBass gene-level burden test results. Aggregated gene-level rare variant "
            "association results from whole-exome sequencing of 394,841 UK Biobank participants."
        ),
        "author": "Genebass",
        "publication_date": "2022-09-14",
        "trait_type": "mixed",
        "data_type": "gene_based",
        "n_samples": 394841,
        "metadata_file": "gs://daly-genetics-results/mapping_files/genebass_pheno_results.txt.bgz",
        "metadata_harmonizer": "genebass",
    },
    # expression
    "gtex_expression": {
        "resource": "gtex",
        "version": "v10",
        "description": "GTEx v10 median TPM gene expression across 54 human tissues from 948 post-mortem donors.",
        "author": "GTEx Consortium",
        "publication_date": "2024-11-04",
        "trait_type": None,
        "data_type": "expression",
        "n_samples": 948,
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "hpa_expression": {
        "resource": "hpa",
        "version": "v24.1",
        "description": "Human Protein Atlas v24.1 immunohistochemistry-based protein expression across human tissues and cell types.",
        "author": "Human Protein Atlas",
        "publication_date": "2024-11-18",
        "trait_type": None,
        "data_type": "expression",
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    # chromatin peaks
    "finngen_chromatin_peaks": {
        "resource": "finngen",
        "version": "batch1-5",
        "description": "FinnGen single-cell ATAC-seq peak-to-gene associations. Links chromatin accessibility peaks to target genes across immune cell types.",
        "author": "FinnGen Consortium",
        "publication_date": "2025-12-15",
        "trait_type": None,
        "data_type": "chromatin_peaks",
        "n_samples": 1108,
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    # gene-disease
    "gencc": {
        "resource": "gencc",
        "version": "2025-11-13",
        "description": "GenCC (Gene Curation Coalition) gene-disease validity classifications. Curated submissions from ClinGen, DECIPHER, Genomics England PanelApp, and other panels.",
        "author": "GenCC",
        "publication_date": "2025-11-13",
        "trait_type": None,
        "data_type": "gene_disease",
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
    "monarch": {
        "resource": "monarch",
        "version": "2025-12-07",
        "description": "Monarch Initiative causal gene-to-disease associations. Integrated Mendelian disease-gene relationships from OMIM, Orphanet, and other sources.",
        "author": "Monarch Initiative",
        "publication_date": "2025-12-07",
        "trait_type": None,
        "data_type": "gene_disease",
        "metadata_file": None,
        "metadata_harmonizer": None,
    },
}
