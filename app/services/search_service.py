import logging
from typing import Literal, TYPE_CHECKING
import polars as pl
from rapidfuzz import fuzz, process
from app.services.config_util import get_datasets, get_resources_with_metadata

if TYPE_CHECKING:
    from app.services.data_access import DataAccess

logger = logging.getLogger(__name__)


class SearchIndex:
    """
    Service for fast in-memory search and autocomplete.
    Indexes phenotypes and genes at startup for fuzzy search.
    """

    def __init__(self, hgnc_file: str, data_access: "DataAccess | None" = None):
        self.phenotypes = []
        self.genes = []
        self.search_items = []
        self._hgnc_file = hgnc_file
        self._data_access = data_access
        self._initialize()

    def _initialize(self):
        """Load all searchable data at startup"""
        logger.info("Initializing search index...")
        self._load_phenotypes()
        self._load_genes()
        logger.info(
            f"Search index initialized: {len(self.phenotypes)} phenotypes, {len(self.genes)} genes"
        )

    def _load_phenotypes(self):
        """Load phenotypes from all metadata files"""
        if self._data_access is None:
            from app.services.data_access import DataAccess
            self._data_access = DataAccess()

        for resource in get_resources_with_metadata():
            try:
                # get harmonized metadata which handles configs internally
                harmonized_dicts = self._data_access.get_harmonized_metadata(resource)
                logger.debug(f"Loading {len(harmonized_dicts)} phenotypes from {resource}")

                for item_dict in harmonized_dicts:
                    code = item_dict.get("phenotype_code")
                    name = item_dict.get("phenotype_string")

                    # handle sample_size which can be int or "NA"
                    n_samples = item_dict.get("n_samples", 0)
                    if isinstance(n_samples, str) and n_samples == "NA":
                        sample_size = 0
                    else:
                        sample_size = n_samples

                    if code and name:
                        phenotype = {
                            "type": "phenotype",
                            "code": code,
                            "name": name,
                            "resource": resource,
                            "sample_size": sample_size,
                            # store normalized search strings
                            "search_strings": [
                                code.lower(),
                                name.lower(),
                            ],
                        }
                        self.phenotypes.append(phenotype)
                        # add to flat search items list for rapidfuzz
                        self.search_items.append(
                            {
                                "item": phenotype,
                                "search_key": f"{code} {name}",
                                "primary": code,
                            }
                        )

            except Exception as e:
                logger.error(f"Error loading phenotypes from {resource}: {e}")
                raise e

        # load inline phenotypes from datasets (e.g. external sumstats)
        for dataset_id, entry in get_datasets().items():
            inline_phenos = entry.get("phenotypes")
            if not inline_phenos:
                continue
            resource = entry.get("resource", dataset_id)
            for item_dict in inline_phenos:
                code = item_dict.get("phenotype_code")
                name = item_dict.get("phenotype_string")
                n_samples = item_dict.get("n_samples", 0)
                if code and name:
                    phenotype = {
                        "type": "phenotype",
                        "code": code,
                        "name": name,
                        "resource": resource,
                        "sample_size": n_samples,
                        "search_strings": [code.lower(), name.lower()],
                    }
                    self.phenotypes.append(phenotype)
                    self.search_items.append(
                        {
                            "item": phenotype,
                            "search_key": f"{code} {name}",
                            "primary": code,
                        }
                    )

    def _load_genes(self):
        """Load genes from HGNC complete set"""
        try:
            logger.debug(f"Loading genes from {self._hgnc_file}")

            # read with infer_schema_length=0 to treat all columns as strings
            # to avoid parsing errors with pipe-delimited values
            df = pl.read_csv(self._hgnc_file, separator="\t", infer_schema_length=0)
            logger.debug(f"Loaded {len(df)} genes from HGNC")

            for row in df.iter_rows(named=True):
                symbol = row.get("symbol")
                name = row.get("name")
                ensembl_id = row.get("ensembl_gene_id")

                if not symbol:
                    continue

                # parse aliases and previous symbols
                alias_symbol = row.get("alias_symbol") or ""
                prev_symbol = row.get("prev_symbol") or ""
                aliases = []
                if alias_symbol and alias_symbol != "":
                    aliases.extend(alias_symbol.split("|"))
                if prev_symbol and prev_symbol != "":
                    aliases.extend(prev_symbol.split("|"))

                gene = {
                    "type": "gene",
                    "symbol": symbol,
                    "name": name or "",
                    "aliases": aliases,
                    "ensembl_id": ensembl_id or "",
                    "search_strings": [symbol.lower()]
                    + [name.lower() if name else ""]
                    + [a.lower() for a in aliases if a]
                    + [ensembl_id.lower() if ensembl_id else ""],
                }
                self.genes.append(gene)

                # add symbol as primary search key
                self.search_items.append(
                    {
                        "item": gene,
                        "search_key": symbol,
                        "primary": symbol,
                        "is_symbol": True,
                    }
                )

                # add name if different from symbol
                if name and name.lower() != symbol.lower():
                    self.search_items.append(
                        {
                            "item": gene,
                            "search_key": name,
                            "primary": symbol,
                            "is_symbol": False,
                        }
                    )

                # add aliases
                for alias in aliases:
                    if alias and alias.lower() != symbol.lower():
                        self.search_items.append(
                            {
                                "item": gene,
                                "search_key": alias,
                                "primary": symbol,
                                "is_symbol": False,
                            }
                        )

                # add Ensembl ID as searchable
                if ensembl_id and ensembl_id.lower() != symbol.lower():
                    self.search_items.append(
                        {
                            "item": gene,
                            "search_key": ensembl_id,
                            "primary": symbol,
                            "is_symbol": False,  # Ensembl ID is not the official symbol
                        }
                    )

        except Exception as e:
            logger.error(f"Error loading genes: {e}")
            raise e

    def search(
        self,
        query: str,
        limit: int = 10,
        types: list[Literal["phenotypes", "genes"]] | None = None,
    ) -> list[dict]:
        """
        Search for phenotypes and genes with fuzzy matching.

        Args:
            query: Search query string
            limit: Maximum number of results to return
            types: Filter by types (None = both)

        Returns:
            List of matching items with scores and match types
        """
        if not query or len(query.strip()) == 0:
            return []

        query = query.strip()

        # filter search items by type
        if types:
            filtered_items = []
            for search_item in self.search_items:
                item_type = search_item["item"]["type"]
                if (item_type == "phenotype" and "phenotypes" in types) or (
                    item_type == "gene" and "genes" in types
                ):
                    filtered_items.append(search_item)
        else:
            filtered_items = self.search_items

        if not filtered_items:
            return []

        # use rapidfuzz for fuzzy matching
        # WRatio gives best results for partial matches and different lengths
        matches = process.extract(
            query,
            [item["search_key"] for item in filtered_items],
            scorer=fuzz.WRatio,
            limit=limit * 3,  # get more candidates for re-ranking
            score_cutoff=60,  # minimum similarity threshold
        )

        # re-rank matches based on our criteria
        ranked_results = []
        seen_items = set()  # deduplicate items matched via different keys

        for match_text, score, match_idx in matches:
            search_item = filtered_items[match_idx]
            item = search_item["item"]
            item_id = id(item)

            # deduplicate
            if item_id in seen_items:
                continue
            seen_items.add(item_id)

            # determine match type
            query_lower = query.lower()
            search_key_lower = search_item["search_key"].lower()
            primary_lower = search_item["primary"].lower()

            if query_lower == search_key_lower:
                match_type = "exact"
                rank_score = 1000 + score
            elif search_key_lower.startswith(query_lower):
                match_type = "prefix"
                rank_score = 900 + score
            elif query_lower in search_key_lower:
                match_type = "contains"
                rank_score = 800 + score
            else:
                match_type = "fuzzy"
                rank_score = score

            # boost genes if matched on symbol vs alias
            if item["type"] == "gene" and search_item.get("is_symbol", False):
                rank_score += 100

            # boost phenotypes by sample size (normalized)
            if item["type"] == "phenotype":
                sample_size = item.get("sample_size", 0)
                # add up to 50 points based on log of sample size
                # handle both int and potential "NA" string
                if isinstance(sample_size, int) and sample_size > 0:
                    import math

                    rank_score += min(50, math.log10(sample_size) * 10)

            result = {
                **item,
                "match_type": match_type,
                "match_score": score,
                "rank_score": rank_score,
                "matched_key": search_item["search_key"],
            }
            ranked_results.append(result)

        # sort by rank_score (desc), then alphabetically by primary key
        ranked_results.sort(
            key=lambda x: (-x["rank_score"], x.get("code") or x.get("symbol", ""))
        )

        # return top N results
        return ranked_results[:limit]
