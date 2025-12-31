from app.config.common import dataset_to_resource, dataset_mapping_files
import fsspec
import logging

logger = logging.getLogger(__name__)


class DatasetMapping:
    def __init__(self) -> None:
        self._init_dataset_to_resource_mapping()

    def _init_dataset_to_resource_mapping(self) -> None:
        self.dataset_to_resource_version = dataset_to_resource
        if dataset_mapping_files:
            for path, key, resource, version in dataset_mapping_files:
                # use fsspec to support both local files and gs:// URLs
                with fsspec.open(path, "rt") as f:
                    header = f.readline().strip().split("\t")
                    key_index = header.index(key)
                    for line in f:
                        s = line.strip().split("\t")
                        self.dataset_to_resource_version[s[key_index]] = (
                            resource,
                            version,
                        )
        self.dataset_to_resource_version_bytes = {}
        for dataset, (resource, version) in self.dataset_to_resource_version.items():
            self.dataset_to_resource_version_bytes[dataset.encode()] = (
                resource.encode(),
                version.encode(),
            )
        logger.info(
            f"Dataset to resource mapping initialized with {len(self.dataset_to_resource_version)} entries"
        )

    def get_resource_and_version_bytes_by_dataset(
        self, dataset: bytes
    ) -> tuple[bytes, bytes]:
        return self.dataset_to_resource_version_bytes.get(
            dataset, (b"unknown", b"unknown")
        )
