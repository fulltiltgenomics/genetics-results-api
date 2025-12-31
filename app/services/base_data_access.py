from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Any

T = TypeVar("T")  # The DAO type


class BaseDataAccessObject(ABC):
    """
    Base class for all data access objects.

    Provides common initialization pattern and defines the minimal interface
    that all DAOs should implement.
    """

    def __init__(self, identifier: str):
        """
        Initialize the data access object.

        Args:
            identifier: The resource name, dataset name, or other unique identifier
        """
        self.identifier = identifier

    @abstractmethod
    def get_primary_header(self) -> list[bytes]:
        """
        Get the primary header for this data source.

        For single-file data sources, this is the only header.
        For multi-file data sources, this is the main/primary file's header.

        Returns:
            List of header column names as bytes
        """
        pass


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

    @abstractmethod
    def get_implementation_class(self, data_source: str) -> type:
        """
        Get the implementation class for the given data source.

        Args:
            data_source: The data source type (e.g., "gcloud", "local")

        Returns:
            The class to instantiate for this data source

        Raises:
            ValueError: If data source is unknown
        """
        pass

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
