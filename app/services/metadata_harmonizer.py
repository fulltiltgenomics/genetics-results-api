"""
Service for harmonizing phenotype metadata across different resources.

Provides a unified schema for metadata from FinnGen, eQTL Catalogue, and Open Targets.
"""

import logging
from dataclasses import dataclass, asdict
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass
class HarmonizedMetadata:
    """Unified metadata schema across all resources."""

    phenotype_code: str
    phenotype_string: str
    n_samples: int | str  # int or "NA"
    n_cases: int | str  # int or "NA"
    n_controls: int | str  # int or "NA"
    trait_type: Literal["binary", "quantitative"]
    author: str
    date: str  # ISO 8601 format (YYYY-MM-DD)
    resource: str
    version: str

    def to_dict(self):
        """Convert to dictionary."""
        return asdict(self)


class MetadataHarmonizer:
    """Harmonizes metadata from different resources into a unified format."""

    def harmonize_metadata(
        self, resource: str, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """
        Harmonize metadata from a specific resource.

        Args:
            resource: Resource identifier
            raw_metadata: Raw metadata from the resource
            config: Data file configuration with metadata settings

        Returns:
            List of harmonized metadata objects
        """
        if not raw_metadata:
            return []

        # get harmonization config
        harm_config = config.get("metadata", {})
        harm_type = harm_config.get("type", resource)

        # route to appropriate harmonizer based on type
        if harm_type == "finngen_r13":
            return self._harmonize_finngen_r13(raw_metadata, harm_config)
        elif harm_type == "finngen_kanta":
            return self._harmonize_finngen_kanta(raw_metadata, harm_config)
        elif harm_type == "finngen_drugs":
            return self._harmonize_finngen_drugs(raw_metadata, harm_config)
        elif harm_type == "eqtl_catalogue":
            return self._harmonize_eqtl_catalogue(raw_metadata, harm_config)
        elif harm_type == "open_targets":
            return self._harmonize_open_targets(raw_metadata, harm_config)
        elif harm_type == "genebass":
            return self._harmonize_genebass(raw_metadata, harm_config)
        else:
            logger.warning(f"Unknown harmonization type: {harm_type}")
            return []

    def _harmonize_finngen_r13(
        self, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """Harmonize FinnGen R13 GWAS metadata."""
        harmonized = []

        author = config.get("author", "FinnGen Consortium")
        pub_date = config.get("publication_date", "2025-09-01")
        version = config.get("version_label", "R13")

        for item in raw_metadata:
            try:
                num_cases = item.get("num_cases", 0)
                num_controls = item.get("num_controls", 0)

                harmonized.append(
                    HarmonizedMetadata(
                        phenotype_code=item.get("phenocode", ""),
                        phenotype_string=item.get("phenostring", ""),
                        n_samples=num_cases + num_controls,
                        n_cases=num_cases,
                        n_controls=num_controls,
                        trait_type="binary",
                        author=author,
                        date=pub_date,
                        resource="finngen",
                        version=version,
                    )
                )
            except Exception as e:
                logger.error(f"Error harmonizing FinnGen R13 item: {e}")
                continue

        return harmonized

    def _harmonize_finngen_kanta(
        self, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """Harmonize FinnGen Kanta lab test metadata."""
        harmonized = []

        author = config.get("author", "FinnGen Consortium")
        pub_date = config.get("publication_date", "2025-03-01")
        version = config.get("version_label", "kanta")

        for item in raw_metadata:
            try:
                analysis_type = item.get("AnalysisType", "Quantitative")

                # handle string values from TSV
                def safe_int(value, default=0):
                    if value in ("NA", "", None):
                        return default
                    try:
                        return int(value)
                    except (ValueError, TypeError):
                        return default

                n_total = safe_int(item.get("N_total"))
                n_cases = safe_int(item.get("N_cases"))
                n_controls = safe_int(item.get("N_controls"))

                # determine trait type
                trait_type = (
                    "quantitative" if analysis_type == "Quantitative" else "binary"
                )

                harmonized.append(
                    HarmonizedMetadata(
                        phenotype_code=item.get("OMOPID", ""),
                        phenotype_string=item.get("phenostring", ""),
                        n_samples=n_total,
                        n_cases=n_cases,
                        n_controls=n_controls,
                        trait_type=trait_type,
                        author=author,
                        date=pub_date,
                        resource="finngen",
                        version=version,
                    )
                )
            except Exception as e:
                logger.error(f"Error harmonizing FinnGen Kanta item: {e}")
                continue

        return harmonized

    def _harmonize_finngen_drugs(
        self, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """Harmonize FinnGen drugs metadata (quantitative, no sample sizes)."""
        harmonized = []

        author = config.get("author", "FinnGen Consortium")
        pub_date = config.get("publication_date", "2024-01-01")
        version = config.get("version_label", "R12")

        for item in raw_metadata:
            try:
                harmonized.append(
                    HarmonizedMetadata(
                        phenotype_code=item.get("phenocode", ""),
                        phenotype_string=item.get("phenostring", ""),
                        n_samples="NA",
                        n_cases="NA",
                        n_controls="NA",
                        trait_type="quantitative",
                        author=author,
                        date=pub_date,
                        resource="finngen",
                        version=version,
                    )
                )
            except Exception as e:
                logger.error(f"Error harmonizing FinnGen drugs item: {e}")
                continue

        return harmonized

    def _harmonize_eqtl_catalogue(
        self, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """Harmonize eQTL Catalogue metadata."""
        harmonized = []

        pub_date = config.get("publication_date", "2020-01-01")
        version = config.get("version_label", "R7")

        for item in raw_metadata:
            try:
                # construct descriptive phenotype string
                sample_group = item.get("sample_group", "")
                tissue_label = item.get("tissue_label", "")
                condition_label = item.get("condition_label", "")

                # combine into meaningful description
                parts = [p for p in [sample_group, tissue_label, condition_label] if p]
                phenotype_string = " - ".join(parts) if parts else sample_group

                # eQTL studies use study_label as author
                author = item.get("study_label", "eQTL Catalogue")

                # get sample size, use "NA" if not available
                sample_size_str = item.get("sample_size", "")
                try:
                    n_samples = int(sample_size_str) if sample_size_str else "NA"
                except (ValueError, TypeError):
                    n_samples = "NA"

                harmonized.append(
                    HarmonizedMetadata(
                        phenotype_code=item.get("dataset_id", ""),
                        phenotype_string=phenotype_string,
                        n_samples=n_samples,
                        n_cases="NA",  # QTL data doesn't have cases/controls
                        n_controls="NA",
                        trait_type="quantitative",
                        author=author,
                        date=pub_date,
                        resource="eqtl_catalogue",
                        version=version,
                    )
                )
            except Exception as e:
                logger.error(f"Error harmonizing eQTL Catalogue item: {e}")
                continue

        return harmonized

    def _harmonize_open_targets(
        self, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """Harmonize Open Targets metadata."""
        harmonized = []

        default_pub_date = config.get("publication_date", "unknown")
        version = config.get("version_label", "unknown")

        for item in raw_metadata:
            try:
                # helper to convert to int or "NA"
                def safe_int_or_na(value):
                    if value in (None, "", "NA"):
                        return "NA"
                    try:
                        val = int(value)
                        return val if val > 0 else "NA"
                    except (ValueError, TypeError):
                        return "NA"

                n_samples = safe_int_or_na(item.get("nSamples"))
                n_cases = safe_int_or_na(item.get("nCases"))
                n_controls = safe_int_or_na(item.get("nControls"))

                # determine trait type based on presence of cases
                trait_type = "binary" if (n_cases != "NA" and n_cases > 0) else "quantitative"

                # use publication date if available, otherwise use default
                pub_date = item.get("publicationDate", "")
                if not pub_date or pub_date == "":
                    pub_date = default_pub_date

                # use first author or default
                author = item.get("publicationFirstAuthor", "Open Targets")

                harmonized.append(
                    HarmonizedMetadata(
                        phenotype_code=item.get("studyId", ""),
                        phenotype_string=item.get("traitFromSource", ""),
                        n_samples=n_samples,
                        n_cases=n_cases,
                        n_controls=n_controls,
                        trait_type=trait_type,
                        author=author,
                        date=pub_date,
                        resource="open_targets",
                        version=version,
                    )
                )
            except Exception as e:
                logger.error(f"Error harmonizing Open Targets item: {e}")
                continue

        return harmonized

    def _harmonize_genebass(
        self, raw_metadata: list[dict], config: dict
    ) -> list[HarmonizedMetadata]:
        """Harmonize GeneBass metadata."""
        harmonized = []

        author = config.get("author", "GeneBass")
        pub_date = config.get("publication_date", "2022-01-01")
        version = config.get("version_label", "500k")

        for item in raw_metadata:
            try:
                def safe_int_or_na(value):
                    if value in ("NA", None, ""):
                        return "NA"
                    try:
                        val = int(value)
                        return val if val > 0 else "NA"
                    except (ValueError, TypeError):
                        return "NA"

                n_cases = safe_int_or_na(item.get("n_cases"))
                n_controls = safe_int_or_na(item.get("n_controls"))
                n_cases_defined = safe_int_or_na(item.get("n_cases_defined"))

                # use n_cases_defined if available, otherwise try to sum cases+controls
                if n_cases_defined != "NA":
                    n_samples = n_cases_defined
                elif n_cases != "NA" and n_controls != "NA":
                    n_samples = n_cases + n_controls
                else:
                    n_samples = "NA"

                # determine trait type from trait_type column
                raw_trait_type = item.get("trait_type", "")
                trait_type = "quantitative" if raw_trait_type in ("continuous", "quantitative") else "binary"

                # construct phenotype_code as: trait_type_phenocode_pheno_sex_coding_modifier
                phenotype_code = "_".join([
                    str(item.get("trait_type", "")),
                    str(item.get("phenocode", "")),
                    str(item.get("pheno_sex", "")),
                    str(item.get("coding", "")),
                    str(item.get("modifier", "")),
                ])

                # use description column for human-readable name
                # include coding_description if available, fall back to phenocode if no description
                description = item.get("description", "")
                coding_description = item.get("coding_description", "")
                if not description or description == "NA":
                    phenotype_string = str(item.get("phenocode", ""))
                elif coding_description and coding_description != "NA":
                    phenotype_string = f"{description}: {coding_description}"
                else:
                    phenotype_string = description

                harmonized.append(
                    HarmonizedMetadata(
                        phenotype_code=phenotype_code,
                        phenotype_string=phenotype_string,
                        n_samples=n_samples,
                        n_cases=n_cases,
                        n_controls=n_controls,
                        trait_type=trait_type,
                        author=author,
                        date=pub_date,
                        resource="genebass",
                        version=version,
                    )
                )
            except Exception as e:
                logger.error(f"Error harmonizing GeneBass item: {e}")
                continue

        return harmonized
