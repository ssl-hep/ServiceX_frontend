from typing import List, Protocol

from servicex.models import CachedDataset, TransformRequest, TransformStatus


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
