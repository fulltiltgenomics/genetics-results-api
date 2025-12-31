import logging
import polars as pl
from app.config.gene_disease import gene_disease
from app.core.exceptions import DataException

logger = logging.getLogger(__name__)


class GeneDiseaseData:
    def __init__(self) -> None:
        self._init_gene_disease_data()

    def _init_gene_disease_data(self) -> None:
        """Load and harmonize gene-to-disease data from multiple sources."""
        try:
            output_columns = gene_disease["output_columns"]
            dataframes = []

            gencc_config = gene_disease["gencc"]
            logger.info(f"Loading GenCC data from {gencc_config['file']}")
            gencc_df = pl.read_csv(
                gencc_config["file"],
                separator="\t",
                null_values=["", "NA"],
            )

            # select and rename columns from GenCC
            gencc_mapping = gencc_config["columns"]
            gencc_df = gencc_df.select(
                [
                    pl.col(source_col).alias(target_col)
                    for target_col, source_col in gencc_mapping.items()
                ]
            )

            # add resource column
            gencc_df = gencc_df.with_columns(pl.lit("gencc").alias("resource"))

            # add missing columns as NA
            for col in output_columns:
                if col not in gencc_df.columns:
                    gencc_df = gencc_df.with_columns(pl.lit("NA").alias(col))

            dataframes.append(gencc_df.select(output_columns))
            logger.info(f"Loaded {len(gencc_df)} GenCC records")

            monarch_config = gene_disease["monarch"]
            logger.info(f"Loading Monarch data from {monarch_config['file']}")
            monarch_df = pl.read_csv(
                monarch_config["file"],
                separator="\t",
                null_values=["", "NA"],
            ).with_columns(
                pl.concat_str(
                    pl.col("subject"),
                    pl.col("object"),
                    pl.col("primary_knowledge_source"),
                    separator="|",
                ).alias("uuid")
            )

            # select and rename columns from Monarch
            monarch_mapping = monarch_config["columns"]
            monarch_df = monarch_df.select(
                [
                    pl.col(source_col).alias(target_col)
                    for target_col, source_col in monarch_mapping.items()
                ]
            )

            # add resource column
            monarch_df = monarch_df.with_columns(pl.lit("monarch").alias("resource"))

            # add missing columns as NA
            for col in output_columns:
                if col not in monarch_df.columns:
                    monarch_df = monarch_df.with_columns(pl.lit("NA").alias(col))

            dataframes.append(monarch_df.select(output_columns))
            logger.info(f"Loaded {len(monarch_df)} Monarch records")

            # combine both datasets
            self.data = pl.concat(dataframes, how="vertical")

            # create an index on gene_symbol for faster lookups
            # convert to uppercase for case-insensitive matching
            self.data = self.data.with_columns(
                pl.col("gene_symbol").str.to_uppercase().alias("gene_symbol_upper")
            )

            logger.info(
                f"Loaded {len(self.data)} total gene-disease records with "
                f"{self.data['gene_symbol'].n_unique()} unique genes"
            )

        except Exception as e:
            logger.error(f"Error loading gene-disease data: {e}")
            raise DataException(f"Error loading gene-disease data: {e}")

    def get_by_gene_symbol(self, gene_symbol: str) -> pl.DataFrame:
        """
        Get gene-disease records for a specific gene.

        Args:
            gene_symbol: Gene symbol to search for (case-insensitive)

        Returns:
            Polars DataFrame with matching records
        """
        gene_symbol_upper = gene_symbol.upper()

        filtered = self.data.filter(
            pl.col("gene_symbol_upper") == gene_symbol_upper
        ).drop("gene_symbol_upper")

        return filtered
