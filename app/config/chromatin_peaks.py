"""
Chromatin peaks data configuration including schemas and data sources.

This module contains all configuration related to chromatin peaks data:
- Header schema for chromatin peaks data validation
- Chromatin peaks data source configurations
"""

chromatin_peaks_header_schema = {
    "resource": str,
    "version": str,
    "chrom": str,  # can be "chr1" format
    "start": int,
    "end": int,
    "peak_id": str,
    "gene_id": str,
    "symbol": str,
    "cell_type": str,
    "total_cell_num": int,
    "expr_cell_num": int,
    "open_cell_num": int,
    "hurdle_zero_beta": float,
    "hurdle_zero_se": float,
    "hurdle_zero_z": float,
    "hurdle_zero_nlog10p": float,
    "hurdle_count_beta": float,
    "hurdle_count_se": float,
    "hurdle_count_z": float,
    "hurdle_count_nlog10p": float,
    "hurdle_aic": float,
    "hurdle_bic": float,
}

chromatin_peaks_data = [
    {
        "data_source": "gcloud",
        "resource": "finngen",
        "version": "R12",
        "file": "gs://cascade-browser/cascade_results/open4gene.all.results.sig.tsv.gz",
    },
]
