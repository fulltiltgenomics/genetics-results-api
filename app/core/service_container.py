"""
Centralized service container for dependency injection.
"""

import asyncio
import logging
import threading
from enum import Enum
from typing import TypeVar, Callable, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Warm(Enum):
    """What startup warming does with a service. Declared at registration, nowhere else.

    The alternative — a list of names in the lifespan — is a second list with nothing
    comparing it to the first, so a service added here is silently left cold and only a
    slow first request says so.
    """

    # construction does blocking, network-bound work, so startup builds it in a worker
    # thread. a factory that resolves another service takes THAT name's lock, never its
    # own, so declaring a service and its dependency both THREAD constructs each once.
    THREAD = "thread"

    # construction must be cheap and non-blocking — it happens on the loop — after which
    # `await warm_all()` prefetches over the network.
    #
    # every warm_all() swallows its per-file failures, and that is deliberate rather than
    # an oversight: warm_all is a prefetch, not a check. startup_checks.verify_all_data_files()
    # has already run (run_server.py, before uvicorn) and is the authoritative reachability
    # gate, so re-raising here would either restate its verdict or abort startup over a file
    # it deliberately tolerates. do not "tidy" these into raising.
    ASYNC = "async"

    # deliberately cold; the registration must say why.
    NONE = "none"


class ServiceContainer:
    """
    Container for managing service instances.

    Services are lazily instantiated on first access and cached for reuse.
    """

    def __init__(self):
        self._instances: dict[str, Any] = {}
        self._factories: dict[str, Callable[[], Any]] = {}
        self._warm: dict[str, Warm] = {}
        # per-name locks so startup warming can create independent singletons in
        # parallel threads without double-constructing any single one. A factory
        # may resolve other services (e.g. search_index needs gene_name_mapping):
        # that acquires a DIFFERENT name's lock, so no thread re-enters the same
        # lock and there is no deadlock.
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def register(
        self,
        name: str,
        factory: Callable[[], Any],
        warm: Warm,
        reason: str = "",
    ) -> None:
        """Register a factory function for creating a service.

        `warm` has no default on purpose: registering a service without deciding whether
        startup warms it must be impossible, not merely discouraged. `reason` is required
        for Warm.NONE — that is the choice a later reader cannot reconstruct.
        """
        if not isinstance(warm, Warm):
            raise TypeError(f"service '{name}': warm must be a Warm, got {warm!r}")
        if warm is Warm.NONE and not reason:
            raise ValueError(f"service '{name}' is declared Warm.NONE without a reason")
        self._factories[name] = factory
        self._warm[name] = warm

    def _lock_for(self, name: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(name)
            if lock is None:
                lock = threading.Lock()
                self._locks[name] = lock
            return lock

    def get(self, name: str) -> Any:
        """Get a service instance, creating it if necessary (thread-safe)."""
        instance = self._instances.get(name)
        if instance is not None:
            return instance
        if name not in self._factories:
            raise KeyError(f"Service '{name}' not registered")
        with self._lock_for(name):
            # re-check under the lock; another thread may have created it
            if name not in self._instances:
                logger.debug(f"Creating service instance: {name}")
                self._instances[name] = self._factories[name]()
            return self._instances[name]

    def warm_names(self, kind: Warm) -> list[str]:
        """Registered services declared with `kind`, in registration order."""
        return [name for name, warm in self._warm.items() if warm is kind]

    def warm_policy(self, name: str) -> Warm:
        return self._warm[name]

    async def warm_registered(self) -> None:
        """Run exactly the warming the registrations declare, concurrently.

        Startup therefore takes the slowest branch rather than their sum, and the set of
        branches is derived here rather than restated by the caller.
        """
        # the ASYNC services are constructed here, on the loop and before the threads are
        # dispatched: their factories are cheap by declaration, and building them first
        # means a THREAD factory that resolves one (search_index needs data_access) finds
        # it built instead of contending with the loop for that name's lock.
        async_services = [self.get(name) for name in self.warm_names(Warm.ASYNC)]
        await asyncio.gather(
            *(asyncio.to_thread(self.get, name) for name in self.warm_names(Warm.THREAD)),
            *(service.warm_all() for service in async_services),
        )

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

    # search index - depends on hgnc file, data access, and gene name mapping
    def create_search_index():
        from app.services.search_service import SearchIndex
        return SearchIndex(
            config.hgnc_file,
            container.get("data_access"),
            container.get("gene_name_mapping"),
        )

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

    def create_data_access_open_chromatin():
        from app.services.data_access_open_chromatin import DataAccessOpenChromatin
        return DataAccessOpenChromatin()

    def create_data_access_variant_effect():
        from app.services.data_access_variant_effect import DataAccessVariantEffect
        return DataAccessVariantEffect()

    def create_data_access_mpra():
        from app.services.data_access_mpra import DataAccessMpra
        return DataAccessMpra()

    # gene name mapping
    def create_gene_name_mapping():
        from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
        return GeneNameAndPositionMapping()

    # gene group / lineage service
    def create_gene_group_service():
        from app.services.gene_group_service import GeneGroupService
        return GeneGroupService()

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

    def create_variant_annotation_service():
        from app.services.variant_annotation_service import VariantAnnotationService
        return VariantAnnotationService()

    def create_variant_set_service():
        from app.services.variant_set_service import VariantSetService
        return VariantSetService()

    # every registration states whether startup warms it; app.server's lifespan derives
    # its warming from these declarations rather than repeating the names.
    container.register(
        "request_util", create_request_util, Warm.NONE,
        "pure parsing; construction touches nothing",
    )
    container.register("search_index", create_search_index, Warm.THREAD)
    container.register("data_access", create_data_access, Warm.ASYNC)
    container.register("data_access_coloc", create_data_access_coloc, Warm.ASYNC)
    container.register("data_access_expression", create_data_access_expression, Warm.ASYNC)
    container.register(
        "data_access_chromatin_peaks", create_data_access_chromatin_peaks, Warm.ASYNC
    )
    container.register(
        "data_access_open_chromatin", create_data_access_open_chromatin, Warm.ASYNC
    )
    container.register(
        "data_access_variant_effect", create_data_access_variant_effect, Warm.ASYNC
    )
    container.register("data_access_mpra", create_data_access_mpra, Warm.ASYNC)
    container.register("gene_name_mapping", create_gene_name_mapping, Warm.THREAD)
    container.register(
        "gene_group_service", create_gene_group_service, Warm.NONE,
        "blocking: builds its lineage map from three GCS CSVs, so the first gene-group "
        "request pays that read on the event loop. warming it would be safe — the load "
        "swallows every failure — and it is left cold only because the cost falls on one "
        "router; this is a decision to revisit, not a property to rely on",
    )
    container.register("gene_disease_data", create_gene_disease_data, Warm.THREAD)
    # finemapped, metadata, ld_datafetch and datafetch each name a module that is not in
    # app/services/, so those factories raise ImportError the moment anything resolves
    # them; no `container.get` in the tree asks for these names. declared rather than
    # deleted because removing a registration is a separate decision.
    container.register(
        "finemapped", create_finemapped, Warm.NONE,
        "factory imports a module that is not in the tree",
    )
    container.register(
        "rsid_db", create_rsid_db, Warm.NONE,
        "construction only makes the tbi cache dir and primes the process-wide GCS token, "
        "which the warmed services have already done; rsid files are opened per query",
    )
    container.register(
        "metadata", create_metadata, Warm.NONE,
        "factory imports a module that is not in the tree",
    )
    # warmed off the loop rather than left cold: the lifespan smoke query already forces
    # this construction during startup, on the loop thread, inside its blocking fsspec
    # read. declaring it THREAD moves that read into a worker thread and puts it before
    # the smoke query instead of inside it. no request can observe a half-warmed
    # container — uvicorn does not accept connections until lifespan startup returns —
    # and the failure semantics are unchanged, since the smoke query already aborts
    # startup when these files are unreadable.
    container.register("dataset_mapping", create_dataset_mapping, Warm.THREAD)
    container.register(
        "ld_datafetch", create_ld_datafetch, Warm.NONE,
        "factory imports a module that is not in the tree",
    )
    container.register(
        "datafetch", create_datafetch, Warm.NONE,
        "factory imports a module that is not in the tree",
    )
    container.register(
        "phenotype_markdown_service", create_phenotype_markdown_service, Warm.NONE,
        "no construction cost; markdown is fetched per request for one phenocode",
    )
    container.register(
        "credible_set_stats_service", create_credible_set_stats_service, Warm.NONE,
        "no construction cost; stats files are fetched per request",
    )
    container.register(
        "sumstats_data_access", create_sumstats_data_access, Warm.NONE,
        "per-phenotype sumstats files are not enumerable, so there is nothing to prefetch "
        "— the same reason verify_all_data_files() skips them; the GCloudTabixBase init "
        "this service defers is done on first use",
    )
    container.register(
        "variant_annotation_service", create_variant_annotation_service, Warm.NONE,
        "construction reads config only; per-source headers are cached on first use",
    )
    container.register(
        "variant_set_service", create_variant_set_service, Warm.NONE,
        "no construction cost; set files are read per request and verify_all_data_files() "
        "already proves they exist",
    )


# register services on module load
_register_services()
