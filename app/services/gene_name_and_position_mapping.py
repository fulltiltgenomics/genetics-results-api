from typing import Literal
from app.config.genes import genes
import fsspec
from collections import defaultdict as dd
import polars as pl
import logging
from app.core.exceptions import DataException, GeneNotFoundException

logger = logging.getLogger(__name__)


class GeneNameAndPositionMapping:
    def __init__(self) -> None:
        self._init_gene_name_mapping()
        self._init_gene_position_mapping()

    def _init_gene_name_mapping(self) -> None:
        # gene_name_mapping: gene name -> gencode version -> set of ENSG IDs
        # gene names can change across gencode versions
        # and a gene name in one version can map to multiple ENSG IDs
        # those ENSG IDs may have a different gene name in another version
        # so a key in gene_name_mapping maps to all matching ENSG IDs by gencode version
        self.gene_name_mapping = dd(lambda: dd(set))
        # case-insensitive lookup: lowercase gene name -> actual gene name in gene_name_mapping
        self._gene_name_lowercase_to_actual: dict[str, str] = {}
        path = genes["gene_name_mapping_file"]
        gencode_versions = genes["gencode_versions"]
        # ensg -> {version: name}, used for version-aware dedup below
        ensg_names_by_version: dict[str, dict[int, str]] = {}
        # use fsspec to support both local files and gs:// URLs
        with fsspec.open(path, "rt") as f:
            header = f.readline().strip().split("\t")
            for line in f:
                s = line.strip().split("\t")
                ensg = s[header.index("ensg")].strip()
                names_by_version = {
                    version: s[header.index(f"gene_name_{version}")].strip()
                    for version in gencode_versions
                }
                ensg_names_by_version[ensg] = names_by_version
                for anchor in names_by_version.values():
                    if anchor == "NA":
                        continue
                    for version in gencode_versions:
                        if names_by_version[version] != "NA":
                            self.gene_name_mapping[anchor][version].add(ensg)

        # add hgnc alias and prev symbols to gene name mapping
        # for each alias and prev symbol whose real symbol is in the gene name mapping, add the alias and prev symbol with the same content
        hgnc = (
            pl.read_csv(
                genes["hgnc_file"],
                separator="\t",
                null_values=[""],
                infer_schema_length=100000,
            )
            .select(["symbol", "alias_symbol", "prev_symbol"])
            .to_dicts()
        )
        for gene in hgnc:
            if gene["symbol"] in self.gene_name_mapping:
                aliases = (
                    gene["alias_symbol"].replace('"', "").split("|")
                    if gene["alias_symbol"]
                    else []
                )
                prev_symbols = (
                    gene["prev_symbol"].replace('"', "").split("|")
                    if gene["prev_symbol"]
                    else []
                )
                for other_symbol in aliases + prev_symbols:
                    if other_symbol not in self.gene_name_mapping:
                        self.gene_name_mapping[other_symbol] = self.gene_name_mapping[
                            gene["symbol"]
                        ]

        # version-aware dedup: if a gene name maps to multiple ENSGs in a version,
        # keep only the ones that actually have that gene name in that version
        for anchor in list(self.gene_name_mapping.keys()):
            for version in self.gene_name_mapping[anchor].keys():
                ensg_ids = self.gene_name_mapping[anchor][version]
                if len(ensg_ids) > 1:
                    matching = {
                        ensg
                        for ensg in ensg_ids
                        if ensg_names_by_version.get(ensg, {}).get(version) == anchor
                    }
                    if matching:
                        self.gene_name_mapping[anchor][version] = matching

        # build case-insensitive lookup mapping
        for gene_name in self.gene_name_mapping.keys():
            lower = gene_name.lower()
            # if there's a collision, prefer the one that matches the lowercase exactly
            # (e.g., if both "Gene1" and "GENE1" exist, and we're processing them,
            # we store whichever we see, but exact case match takes precedence)
            if lower not in self._gene_name_lowercase_to_actual:
                self._gene_name_lowercase_to_actual[lower] = gene_name

    def _init_gene_position_mapping(self) -> None:
        try:
            self.gene_positions = dd(pl.DataFrame)  # gencode version -> dataframe
            for version in genes["gencode_versions"]:
                self.gene_positions[version] = (
                    (
                        pl.scan_csv(
                            f"{genes['gene_position_file_template'].format(version=version)}",
                            separator="\t",
                            null_values=["NA"],
                        ).with_columns(
                            pl.col("gene_id").str.split(".").list.get(0).alias("ensg")
                        )
                    )
                    .join(
                        pl.scan_csv(
                            genes["hgnc_file"],
                            separator="\t",
                            null_values=[""],
                        )
                        .select(
                            [
                                "symbol",
                                "name",
                                "ensembl_gene_id",
                                "alias_symbol",
                                "prev_symbol",
                            ]
                        )
                        .rename(
                            {
                                "symbol": "hgnc_symbol",
                                "name": "hgnc_name",
                                "alias_symbol": "hgnc_alias_symbol",
                                "prev_symbol": "hgnc_prev_symbol",
                            }
                        ),
                        left_on="ensg",
                        right_on="ensembl_gene_id",
                        how="left",
                    )
                    .collect()
                )

        except Exception as e:
            logger.error(f"Error reading gene position file: {e}")
            raise DataException(f"Error reading gene position file")

    def get_coordinates_by_gene_name(
        self, gene_name: str
    ) -> dict[str, list[dict[str, str | int]]] | None:
        """
        Get the coordinates of a gene by gene name (case-insensitive).
        Returns a dictionary of gencode versions to lists of dictionaries with "chrom", "gene_start", and "gene_end".
        """
        gene_name_upper = gene_name.upper()
        if gene_name_upper.startswith("ENSG"):
            coords = {
                gencode_version: self.gene_positions[gencode_version]
                .filter(pl.col("ensg") == gene_name_upper)
                .select(["chrom", "gene_start", "gene_end"])
                .to_dicts()
                for gencode_version in genes["gencode_versions"]
            }
            return coords

        # try exact match first, then case-insensitive match
        actual_gene_name = gene_name
        if gene_name not in self.gene_name_mapping:
            gene_name_lower = gene_name.lower()
            if gene_name_lower in self._gene_name_lowercase_to_actual:
                actual_gene_name = self._gene_name_lowercase_to_actual[gene_name_lower]
            else:
                raise GeneNotFoundException(f"Gene {gene_name} not found")

        ensg_ids_by_version = self.gene_name_mapping[actual_gene_name]
        coords = {}
        for version in ensg_ids_by_version.keys():
            coords[version] = (
                self.gene_positions[version]
                .filter(pl.col("ensg").is_in(ensg_ids_by_version[version]))
                .select(["chrom", "gene_start", "gene_end"])
                .to_dicts()
            )
        return coords

    def get_genes_in_region(
        self,
        chromosome: int,
        start: int,
        end: int,
        gene_type: Literal["protein_coding", "all"] = "protein_coding",
        gencode_version: str = None,
    ) -> list[dict[str, str | int]]:
        """
        Get the genes in a region.
        """
        if gencode_version is None:
            gencode_version = genes["gencode_versions"][0]

        if gencode_version not in self.gene_positions:
            raise DataException(f"Gencode version {gencode_version} not available")

        genes_in_region = self.gene_positions[gencode_version].filter(
            pl.col("chrom") == chromosome
        )
        if gene_type != "all":
            genes_in_region = genes_in_region.filter(pl.col("gene_type") == gene_type)
        genes_in_region = genes_in_region.filter(
            (pl.col("gene_start") <= end) & (pl.col("gene_end") >= start)
        )
        genes_in_region = genes_in_region.select(
            [
                "gene_name",
                "chrom",
                "gene_start",
                "gene_end",
                "gene_strand",
                "gene_type",
                "hgnc_symbol",
                "hgnc_name",
                "hgnc_alias_symbol",
                "hgnc_prev_symbol",
            ]
        ).to_dicts()
        return genes_in_region

    def get_nearest_genes(
        self,
        chromosome: int,
        position: int,
        n: int = 3,
        max_distance: int = 1000000,
        gene_type: Literal["protein_coding", "all"] = "protein_coding",
        gencode_version: str = None,
        return_hgnc_symbol_if_only_ensg: bool = False,
    ) -> list[dict[str, str | int]]:  # TODO type
        """
        Find the nearest N genes to a given chromosome position.

        Args:
            chromosome: Chromosome (e.g., 1, 23)
            position: Genomic position
            n: Maximum number of genes to return
            max_distance: Maximum distance from position to consider
            gene_type: Type of genes to return (defaults to protein_coding, can also be "all")
            gencode_version: Gencode version to use (defaults to first available)
            return_hgnc_symbol_if_only_ensg: If True, return HGNC symbol if gencode has only ENSG id but HGNC symbol is available, otherwise keep ENSG id
        Returns:
            List of dictionaries containing gene information and distance
            If there is no gene name in the gencode version (just ENSG id), returns HGNC symbol if available, otherwise ENSG id
        """
        if gencode_version is None:
            gencode_version = genes["gencode_versions"][0]

        if gencode_version not in self.gene_positions:
            raise DataException(f"Gencode version {gencode_version} not available")

        chromosome_df = self.gene_positions[gencode_version].filter(
            pl.col("chrom") == chromosome
        )
        if gene_type != "all":
            chromosome_df = chromosome_df.filter(pl.col("gene_type") == gene_type)

        genes_with_distance = (
            chromosome_df.with_columns(
                [
                    # calculate distance: 0 if inside gene, otherwise distance to either start or end
                    pl.when(
                        (pl.col("gene_start") <= position)
                        & (pl.col("gene_end") >= position)
                    )
                    .then(0)
                    .when(position < pl.col("gene_start"))
                    .then(pl.col("gene_start") - position)
                    .otherwise(position - pl.col("gene_end"))
                    .alias("distance")
                ]
            )
            .filter(pl.col("distance") <= max_distance)
            .sort("distance")
            .head(n)
            .select(
                [
                    "gene_name",
                    "gene_start",
                    "gene_end",
                    "gene_strand",
                    "gene_type",
                    "distance",
                    "hgnc_symbol",
                    "hgnc_name",
                    "hgnc_alias_symbol",
                    "hgnc_prev_symbol",
                ]
            )
            .with_columns(  # use HGNC symbol if gencode has only ENSG id but HGNC symbol is available, otherwise keep ENSG id
                pl.when(
                    (pl.col("gene_name").str.starts_with("ENSG"))
                    & (pl.col("hgnc_symbol").is_not_null())
                    & return_hgnc_symbol_if_only_ensg
                )
                .then(pl.col("hgnc_symbol"))
                .otherwise(pl.col("gene_name"))
                .alias("gene_name")
            )
            .to_dicts()
        )

        return genes_with_distance
