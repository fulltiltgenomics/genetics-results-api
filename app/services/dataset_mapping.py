from app.config.common import dataset_to_resource, dataset_mapping_files
from app.core.file_utils import read_file
import logging

logger = logging.getLogger(__name__)


class DatasetMapping:
    def __init__(self) -> None:
        self._init_dataset_to_resource_mapping()

    def _init_dataset_to_resource_mapping(self) -> None:
        self.dataset_to_resource_version = dataset_to_resource
        if dataset_mapping_files:
            for path, key, resource, version in dataset_mapping_files:
                # read_file supports both local files and gs:// URLs (with retry)
                lines = read_file(path).splitlines()
                header = lines[0].strip().split("\t")
                key_index = header.index(key)
                for line in lines[1:]:
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
