import asyncio
import importlib
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Iterable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")  # The DAO type


class BaseDataAccessObject(ABC):
    """
    Base class for all data access objects.

    Provides the common initialization pattern shared by all DAOs.
    """

    def __init__(self, identifier: str):
        """
        Initialize the data access object.

        Args:
            identifier: The resource name, dataset name, or other unique identifier
        """
        self.identifier = identifier


class BaseFactory(ABC):
    """Base factory for creating data access objects based on configuration."""

    @abstractmethod
    def get_config_entry(self, identifier: str, *args) -> dict:
        """
        Get configuration entry for the identifier.

        Args:
            identifier: The resource name or identifier to look up
            *args: Additional arguments for config lookup

        Returns:
            Configuration dictionary for the resource

        Raises:
            ValueError: If resource not found in configuration
        """
        pass

    # data_source -> "module.path:ClassName", imported on first use rather than at module
    # load: each gcloud_tabix_* module imports its DAO from the module that defines the
    # factory, so a top-level import here would be circular
    implementations: dict[str, str] = {}

    def get_implementation_class(self, data_source: str) -> type:
        """The class to instantiate for `data_source`; ValueError if this factory has none."""
        try:
            module_name, class_name = self.implementations[data_source].split(":")
        except KeyError:
            raise ValueError(
                f"Unknown data source '{data_source}' for {type(self).__name__}"
            ) from None
        return getattr(importlib.import_module(module_name), class_name)

    async def create(self, identifier: str, *args, **kwargs) -> Any:
        """
        Generic create method - handles config lookup and instantiation.

        Args:
            identifier: The resource name or identifier
            *args: Additional arguments for config lookup and class instantiation
            **kwargs: Keyword arguments for class instantiation

        Returns:
            Instance of the appropriate data access implementation
        """
        config = self.get_config_entry(identifier, *args)
        data_source = config.get("data_source", "gcloud")
        impl_class = self.get_implementation_class(data_source)
        return impl_class(identifier, *args, **kwargs)


class BaseDataAccess(Generic[T]):
    """Base class for data access orchestration with caching."""

    def __init__(self):
        self._resource_access_objects: dict = {}

    @abstractmethod
    def create_factory(self) -> BaseFactory:
        """
        Return the factory instance for this domain.

        Returns:
            Factory instance that can create data access objects
        """
        pass

    async def _get_resource_access(
        self, key: tuple, *factory_args, **factory_kwargs
    ) -> T:
        """
        Get or create a cached data access object.

        Args:
            key: Tuple used as cache key
            *factory_args: Arguments to pass to factory.create()
            **factory_kwargs: Keyword arguments to pass to factory.create()

        Returns:
            Cached or newly created data access object
        """
        if key not in self._resource_access_objects:
            factory = self.create_factory()
            self._resource_access_objects[key] = await factory.create(
                *factory_args, **factory_kwargs
            )
        return self._resource_access_objects[key]

    async def _warm_each(
        self, label: str, get_access: Callable, keys: Iterable[tuple]
    ) -> None:
        """Construct and warm (header + .tbi prefetch) one access object per key,
        concurrently, so the first request pays no cold-start cost. `get_access` is the
        subclass's own getter and each key is its argument tuple."""

        async def warm(key: tuple) -> None:
            try:
                access = await get_access(*key)
                if hasattr(access, "warm"):
                    await access.warm()
            # swallowed by design, not omission: warm_all prefetches, it does not gate.
            # verify_all_data_files() decides reachability (see Warm.ASYNC in the container).
            except Exception as e:
                logger.warning(f"{label}: warm failed for {'/'.join(map(str, key))}: {e}")

        await asyncio.gather(*(warm(k) for k in keys))
