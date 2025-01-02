from pathlib import Path
from typing import List, Protocol

from servicex.models import CachedDataset, TransformRequest, TransformStatus, ResultFile


class ServiceXAdapterProtocol(Protocol):
    async def get_transforms(self) -> List[TransformStatus]:
        ...

    def get_code_generators(self):
        ...

    async def get_datasets(
        self, did_finder=None, show_deleted=False
    ) -> List[CachedDataset]:
        ...

    async def get_dataset(self, dataset_id=None) -> CachedDataset:
        ...

    async def delete_dataset(self, dataset_id=None) -> bool:
        ...

    async def delete_transform(self, transform_id=None):
        ...

    async def submit_transform(self, transform_request: TransformRequest) -> str:
        ...

    async def get_transform_status(self, request_id: str) -> TransformStatus:
        ...


class MinioAdapterProtocol(Protocol):
    async def list_bucket(self) -> List[ResultFile]:
        ...

    async def download_file(
            self, object_name: str, local_dir: str, shorten_filename: bool = False) -> Path:
        ...

    async def get_signed_url(self, object_name: str) -> str:
        ...

    @classmethod
    def for_transform(cls, transform: TransformStatus) -> 'MinioAdapterProtocol':
        ...

    @classmethod
    def hash_path(cls, file_name: str) -> str:
        ...
