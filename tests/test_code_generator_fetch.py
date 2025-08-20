from unittest.mock import AsyncMock, MagicMock

import pytest

from pathlib import Path
from servicex.dataset_identifier import FileListDataset
from servicex.query_cache import QueryCache
from servicex.query_core import GenericQueryStringGenerator, Query
from servicex.servicex_client import ServiceXClient
from servicex.servicex_adapter import ServiceXAdapter
from tests.test_servicex_dataset import transform_status1, transform_status3


@pytest.mark.asyncio
async def test_codegen_list_fetched_when_not_cached(mocker):
    """Ensure we validate code generators only when submitting a new transform."""
    sx_adapter = AsyncMock(spec=ServiceXAdapter)
    sx_adapter.submit_transform.return_value = "123-456"
    sx_adapter.get_transform_status.side_effect = [transform_status1, transform_status3]
    sx_adapter.get_code_generators_async = AsyncMock(return_value={"uproot": "img"})
    sx_adapter.url = "http://example.com"

    mocker.patch("servicex.minio_adapter.MinioAdapter", return_value=AsyncMock())

    cache = MagicMock(spec=QueryCache)
    cache.get_transform_by_hash.return_value = None
    cache.is_transform_request_submitted.return_value = False
    cache.cache_path_for_transform.return_value = Path("/tmp")
    cache.transformed_results.return_value = MagicMock()

    client = ServiceXClient(config_path="tests/example_config.yaml")
    client.servicex = sx_adapter
    client.query_cache = cache

    q = client.generic_query(
        dataset_identifier=FileListDataset("file.root"),
        query=GenericQueryStringGenerator("1", "uproot"),
    )

    mocker.patch.object(Query, "download_files", AsyncMock(return_value=[]))

    await q.as_files_async(display_progress=False)

    sx_adapter.get_code_generators_async.assert_called_once()


@pytest.mark.asyncio
async def test_codegen_list_skipped_when_cached(mocker):
    """No validation network call should happen when using cached results."""
    sx_adapter = AsyncMock(spec=ServiceXAdapter)
    sx_adapter.get_code_generators_async = AsyncMock()

    cache = MagicMock(spec=QueryCache)
    cached = MagicMock()
    cached.file_list = ["/tmp/data.parquet"]
    cache.get_transform_by_hash.return_value = cached

    client = ServiceXClient(config_path="tests/example_config.yaml")
    client.servicex = sx_adapter
    client.query_cache = cache

    q = client.generic_query(
        dataset_identifier=FileListDataset("file.root"),
        query=GenericQueryStringGenerator("1", "uproot"),
    )

    result = await q.as_files_async(display_progress=False)

    sx_adapter.get_code_generators_async.assert_not_called()
    assert result is cached
