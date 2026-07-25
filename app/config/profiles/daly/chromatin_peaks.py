"""Daly profile: chromatin peaks data paths."""

chromatin_peaks_data = [
    {
        "data_source": "gcloud",
        "resource": "finngen",
        "version": "R12",
        "file": "gs://daly-genetics-results/atacseq/open4gene.all.results.sig.tsv.gz",
        # same rows re-sorted on the linked gene's locus so peaks can be looked up by gene
        "file_by_gene": "gs://daly-genetics-results/atacseq/open4gene.all.results.sig.by_gene.tsv.gz",
    },
]
