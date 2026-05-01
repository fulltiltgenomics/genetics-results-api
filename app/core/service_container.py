"""
Centralized service container for dependency injection.
"""

import logging
from typing import TypeVar, Callable, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ServiceContainer:
    """
    Container for managing service instances.

    Services are lazily instantiated on first access and cached for reuse.
    """

    def __init__(self):
        self._instances: dict[str, Any] = {}
        self._factories: dict[str, Callable[[], Any]] = {}

    def register(self, name: str, factory: Callable[[], Any]) -> None:
        """Register a factory function for creating a service."""
        self._factories[name] = factory

    def get(self, name: str) -> Any:
        """Get a service instance, creating it if necessary."""
        if name not in self._instances:
            if name not in self._factories:
                raise KeyError(f"Service '{name}' not registered")
            logger.debug(f"Creating service instance: {name}")
            self._instances[name] = self._factories[name]()
        return self._instances[name]

    def reset(self, name: str | None = None) -> None:
        """Reset service instance(s) for testing."""
        if name is None:
            self._instances.clear()
        elif name in self._instances:
            del self._instances[name]

    def is_initialized(self, name: str) -> bool:
        """Check if a service has been instantiated."""
        return name in self._instances


# global service container instance
container = ServiceContainer()


def _register_services():
    """Register all service factories."""
    import app.config.common as config

    # request util
    def create_request_util():
        from app.services.request_util import RequestUtil
        return RequestUtil()

    # search index - depends on hgnc file and data access
    def create_search_index():
        from app.services.search_service import SearchIndex
        return SearchIndex(config.hgnc_file, container.get("data_access"))

    # data access services
    def create_data_access():
        from app.services.data_access import DataAccess
        return DataAccess()

    def create_data_access_coloc():
        from app.services.data_access_coloc import DataAccessColoc
        return DataAccessColoc()

    def create_data_access_expression():
        from app.services.data_access_expression import DataAccessExpression
        return DataAccessExpression()

    def create_data_access_chromatin_peaks():
        from app.services.data_access_chromatin_peaks import DataAccessChromatinPeaks
        return DataAccessChromatinPeaks()

    # gene name mapping
    def create_gene_name_mapping():
        from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
        return GeneNameAndPositionMapping()

    # gene disease data
    def create_gene_disease_data():
        from app.services.gene_disease_data import GeneDiseaseData
        return GeneDiseaseData()

    # other services
    def create_finemapped():
        from app.services.finemapped import Finemapped
        return Finemapped()

    def create_rsid_db():
        from app.services.rsid_db import RsidDB
        return RsidDB(config.rsid_db["file"])

    def create_metadata():
        from app.services.metadata import Metadata
        return Metadata()

    def create_dataset_mapping():
        from app.services.dataset_mapping import DatasetMapping
        return DatasetMapping()

    def create_ld_datafetch():
        from app.services.datafetch_ld import LDDatafetch
        return LDDatafetch()

    def create_datafetch():
        from app.services.datafetch import Datafetch
        return Datafetch()

    def create_phenotype_markdown_service():
        from app.services.phenotype_markdown_service import PhenotypeMarkdownService
        return PhenotypeMarkdownService()

    def create_credible_set_stats_service():
        from app.services.credible_set_stats_service import CredibleSetStatsService
        return CredibleSetStatsService()

    def create_sumstats_data_access():
        from app.services.sumstats_data_access import SumstatsDataAccess
        return SumstatsDataAccess()

    # register all services
    container.register("request_util", create_request_util)
    container.register("search_index", create_search_index)
    container.register("data_access", create_data_access)
    container.register("data_access_coloc", create_data_access_coloc)
    container.register("data_access_expression", create_data_access_expression)
    container.register("data_access_chromatin_peaks", create_data_access_chromatin_peaks)
    container.register("gene_name_mapping", create_gene_name_mapping)
    container.register("gene_disease_data", create_gene_disease_data)
    container.register("finemapped", create_finemapped)
    container.register("rsid_db", create_rsid_db)
    container.register("metadata", create_metadata)
    container.register("dataset_mapping", create_dataset_mapping)
    container.register("ld_datafetch", create_ld_datafetch)
    container.register("datafetch", create_datafetch)
    container.register("phenotype_markdown_service", create_phenotype_markdown_service)
    container.register("credible_set_stats_service", create_credible_set_stats_service)
    container.register("sumstats_data_access", create_sumstats_data_access)


# register services on module load
_register_services()
