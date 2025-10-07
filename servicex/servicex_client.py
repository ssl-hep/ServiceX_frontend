# Copyright (c) 2022-2025, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import logging
import shutil
from typing import Optional, List, TypeVar, Any, Mapping, Union, cast
from pathlib import Path

from servicex.configuration import Configuration
from servicex.models import (
    ResultFormat,
    TransformStatus,
    TransformedResults,
    CachedDataset,
)
from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter
from servicex.query_core import (
    Query,
    QueryStringGenerator,
    GenericQueryStringGenerator,
)
from servicex.types import DID
from servicex.dataset_group import DatasetGroup

from make_it_sync import make_sync
from servicex.databinder_models import ServiceXSpec, General, Sample
from collections.abc import Sequence, Coroutine
from enum import Enum
import traceback
from rich.table import Table

T = TypeVar("T")
logger = logging.getLogger(__name__)


class ProgressBarFormat(str, Enum):
    """Specify the way progress bars are displayed."""

    expanded = "expanded"
    """Show progress bars for each `Sample`"""
    compact = "compact"
    """Show one overall summary set of progress bars"""
    none = "none"
    """Show no progress bars at all"""
    default = "expanded"
    """Default (currently the same as "expanded")"""


class ReturnValueException(Exception):
    """An exception occurred at some point while obtaining this result from ServiceX"""

    def __init__(self, exc):
        import copy

        message = "Exception occurred while making ServiceX request.\n" + (
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        )
        super().__init__(message)
        self._exc = copy.copy(exc)


class GuardList(Sequence):
    def __init__(self, data: Union[Sequence, Exception]):
        import copy

        super().__init__()
        if isinstance(data, Exception):
            self._data = ReturnValueException(data)
        else:
            self._data = copy.copy(data)

    def valid(self) -> bool:
        return not isinstance(self._data, Exception)

    def __getitem__(self, index) -> Any:
        if not self.valid():
            data = cast(Exception, self._data)
            raise data
        else:
            data = cast(Sequence, self._data)
            return data[index]

    def __len__(self) -> int:
        if not self.valid():
            data = cast(Exception, self._data)
            raise data
        else:
            data = cast(Sequence, self._data)
            return len(data)

    def __repr__(self):
        if self.valid():
            return repr(self._data)
        else:
            data = cast(ReturnValueException, self._data)
            return f"Invalid GuardList: {repr(data._exc)}"


def _async_execute_and_wait(coro: Coroutine) -> Any:
    import asyncio

    return asyncio.run(coro)


def _load_ServiceXSpec(
    config: Union[ServiceXSpec, Mapping[str, Any], str, Path],
) -> ServiceXSpec:
    if isinstance(config, Mapping):
        logger.debug("Config from dictionary")
        config = ServiceXSpec(**config)
    elif isinstance(config, ServiceXSpec):
        logger.debug("Config from ServiceXSpec")
    elif isinstance(config, str) or isinstance(config, Path):
        logger.debug("Config from file")

        if isinstance(config, str):
            file_path = Path(config)
        else:
            file_path = config

        import sys
        from .yaml_parser import YAML

        yaml = YAML()

        if sys.version_info < (3, 10):
            from importlib_metadata import entry_points
        else:
            from importlib.metadata import entry_points

        plugins = entry_points(group="servicex.query")
        for _ in plugins:
            yaml.register_class(_.load())
        plugins = entry_points(group="servicex.dataset")
        for _ in plugins:
            yaml.register_class(_.load())

        conf = yaml.load(file_path)
        config = ServiceXSpec(**conf)
    else:
        raise TypeError(f"Unknown config type: {type(config)}")

    return config


async def _build_datasets(config, config_path, servicex_name, fail_if_incomplete):
    def get_codegen(_sample: Sample, _general: General):
        if _sample.Codegen is not None:
            return _sample.Codegen
        elif _general.Codegen is not None:
            return _general.Codegen
        elif isinstance(_sample.Query, QueryStringGenerator):
            return _sample.Query.default_codegen
        elif isinstance(_sample.Query, Query):
            return _sample.Query.codegen

    sx = ServiceXClient(backend=servicex_name, config_path=config_path)
    title_length_limit = await sx.servicex.get_servicex_sample_title_limit()
    datasets = []
    for sample in config.Sample:
        sample.validate_title(title_length_limit)
        query = sx.generic_query(
            dataset_identifier=sample.dataset_identifier,
            title=sample.Name,
            codegen=get_codegen(sample, config.General),
            result_format=config.General.OutputFormat.to_ResultFormat(),
            ignore_cache=sample.IgnoreLocalCache,
            query=sample.Query,
            fail_if_incomplete=fail_if_incomplete,
        )
        logger.debug(f"Query string: {query.generate_selection_string()}")
        query.ignore_cache = sample.IgnoreLocalCache

        datasets.append(query)
    return datasets


def _output_handler(
    config: ServiceXSpec,
    requests: List[Query],
    results: List[Union[TransformedResults, Exception]],
):
    matched_results = zip(requests, results)
    if config.General.Delivery == General.DeliveryEnum.URLs:
        out_dict = {
            obj[0].title: GuardList(
                obj[1].signed_url_list if not isinstance(obj[1], Exception) else obj[1]
            )
            for obj in matched_results
        }
    elif config.General.Delivery == General.DeliveryEnum.LocalCache:
        out_dict = {
            obj[0].title: GuardList(
                obj[1].file_list if not isinstance(obj[1], Exception) else obj[1]
            )
            for obj in matched_results
        }

    if config.General.OutputDirectory:
        import yaml as yl

        out_dir = Path(config.General.OutputDirectory).absolute()
        out_dir.mkdir(parents=True, exist_ok=True)
        out_dict_path = Path(out_dir, config.General.OutFilesetName + ".yaml")

        with open(out_dict_path, "w") as f:
            yl.dump(out_dict, f, default_flow_style=False)

    return out_dict


def _get_progress_options(progress_bar: ProgressBarFormat) -> dict:
    """Get progress options based on progress bar format."""
    if progress_bar == ProgressBarFormat.expanded:
        return {}
    elif progress_bar == ProgressBarFormat.compact:
        return {"overall_progress": True}
    elif progress_bar == ProgressBarFormat.none:
        return {"display_progress": False}
    else:
        raise ValueError(f"Invalid value {progress_bar} for progress_bar provided")


def _display_results(out_dict):
    """Display the delivery results using rich styling."""
    from rich import get_console

    console = get_console()

    console.print("\n[bold green]✓ ServiceX Delivery Complete![/bold green]\n")

    table = Table(
        title="Delivered Files", show_header=True, header_style="bold magenta"
    )
    table.add_column("Sample", style="cyan", no_wrap=True)
    table.add_column("File Count", justify="right", style="green")
    table.add_column("Files", style="dim")

    total_files = 0
    for sample_name, files in out_dict.items():
        if isinstance(files, GuardList) and files.valid():
            file_list = list(files)
            file_count = len(file_list)
            total_files += file_count

            # Show first few files with ellipsis if many
            if file_count <= 3:
                files_display = "\n".join(str(f) for f in file_list)
            else:
                files_display = "\n".join(str(f) for f in file_list[:2])
                files_display += f"\n... and {file_count - 2} more files"

            table.add_row(sample_name, str(file_count), files_display)
        else:
            # Handle error case
            table.add_row(
                sample_name, "[red]Error[/red]", "[red]Failed to retrieve files[/red]"
            )

    console.print(table)
    console.print(f"\n[bold blue]Total files delivered: {total_files}[/bold blue]\n")


async def deliver_async(
    spec: Union[ServiceXSpec, Mapping[str, Any], str, Path],
    config_path: Optional[str] = None,
    servicex_name: Optional[str] = None,
    return_exceptions: bool = True,
    fail_if_incomplete: bool = True,
    ignore_local_cache: bool = False,
    progress_bar: ProgressBarFormat = ProgressBarFormat.default,
    display_results: bool = True,
    concurrency: int = 10,
):
    r"""
    Execute a ServiceX query.

    :param spec: The specification of the ServiceX query, either in a dictionary or a
            :py:class:`~servicex.ServiceXSpec` object.
    :param config_path: The filesystem path to search for the `servicex.yaml` or `.servicex` file.
    :param servicex_name: The name of the ServiceX instance, as specified in the configuration
            YAML file (None will give the default backend).
    :param return_exceptions: If something goes wrong, bubble up the underlying exception for
            debugging (as opposed to just having a generic error).
    :param fail_if_incomplete: If :py:const:`True`: if not all input files are transformed, the
            transformation will be marked as a failure and no outputs will be available. If
            :py:const:`False`, a partial file list will be returned.
    :param ignore_local_cache: If :py:const:`True`, ignore the local query cache and always run
            the query on the remote ServiceX instance.
    :param progress_bar: specify the kind of progress bar to show.
            :py:const:`ProgressBarFormat.expanded` (the default) means every :py:class:`Sample`
            will have its own progress bars; :py:const:`ProgressBarFormat.compact` gives one
            summary progress bar for all transformations; :py:const:`ProgressBarFormat.none`
            switches off progress bars completely.
    :param display_results: Specifies whether the results should be displayed to the console.
            Defaults to True.
    :param concurrency: specify how many downloads to run in parallel (default is 8).
    :return: A dictionary mapping the name of each :py:class:`Sample` to a :py:class:`.GuardList`
            with the file names or URLs for the outputs.
    """
    from .minio_adapter import init_s3_config

    init_s3_config(concurrency)
    config = _load_ServiceXSpec(spec)

    if ignore_local_cache or config.General.IgnoreLocalCache:
        for sample in config.Sample:
            sample.IgnoreLocalCache = True

    datasets = await _build_datasets(
        config, config_path, servicex_name, fail_if_incomplete
    )

    group = DatasetGroup(datasets)

    progress_options = _get_progress_options(progress_bar)

    if config.General.Delivery not in [
        General.DeliveryEnum.URLs,
        General.DeliveryEnum.LocalCache,
    ]:
        raise ValueError(
            f"unexpected value for config.general.Delivery: {config.General.Delivery}"
        )

    if config.General.Delivery == General.DeliveryEnum.URLs:
        results = await group.as_signed_urls_async(
            return_exceptions=return_exceptions, **progress_options
        )

    else:
        results = await group.as_files_async(
            return_exceptions=return_exceptions, **progress_options
        )

    output_dict = _output_handler(config, datasets, results)

    if display_results:
        _display_results(output_dict)

    return output_dict


deliver = make_sync(deliver_async)


class ServiceXClient:
    r"""
    Connection to a ServiceX deployment. Instances of this class can deployment
    data from the service and also interact with previously run transformations.
    Instances of this class are factories for `Datasets``
    """

    def __init__(self, backend=None, url=None, config_path=None):
        r"""
        If both `backend` and `url` are unspecified then it will attempt to pick up
        the default backend from `.servicex`

        :param backend: Name of a deployment from the .servicex file
        :param url: Direct URL of a serviceX deployment instead of using .servicex.
                    Can only work with hosts without auth, or the token is found
                    in a file pointed to by the environment variable BEARER_TOKEN_FILE
        :param config_path: Optional path to the `.servicex` file. If not specified,
                    will search in local directory and up in enclosing directories
        """
        self.config = Configuration.read(config_path)
        self.endpoints = self.config.endpoint_dict()

        if not url and not backend:
            if self.config.default_endpoint:
                backend = self.config.default_endpoint
            else:
                # Take the first endpoint from servicex.yaml if default_endpoint is not set
                backend = self.config.api_endpoints[0].name

        if bool(url) == bool(backend):
            raise ValueError("Only specify backend or url... not both")

        if url:
            self.servicex = ServiceXAdapter(url)
        elif backend:
            if backend not in self.endpoints:
                valid_backends = ", ".join(self.endpoints.keys())
                cfg_file = self.config.config_file or ".servicex"
                raise ValueError(
                    f"Backend {backend} not defined in {cfg_file} file. "
                    f"Valid backend names: {valid_backends}"
                )
            self.servicex = ServiceXAdapter(
                self.endpoints[backend].endpoint,
                refresh_token=self.endpoints[backend].token,
            )
        self.query_cache = QueryCache(self.config)
        # Delay fetching the list of code generators until needed to avoid an
        # unnecessary network call when the client is instantiated.
        self._code_generators: dict[str, str] | None = None

    async def get_transforms_async(self) -> List[TransformStatus]:
        r"""
        Retrieve all transforms you have run on the server
        :return: List of Transform status objects
        """
        return await self.servicex.get_transforms()

    get_transforms = make_sync(get_transforms_async)

    async def get_transform_status_async(self, transform_id) -> TransformStatus:
        r"""
        Get the status of a given transform
        :param transform_id: The uuid of the transform
        :return: The current status for the transform
        """
        return await self.servicex.get_transform_status(request_id=transform_id)

    get_transform_status = make_sync(get_transform_status_async)

    def get_datasets(self, did_finder=None, show_deleted=False) -> List[CachedDataset]:
        r"""
        Retrieve all datasets you have run on the server
        :return: List of Query objects
        """
        return _async_execute_and_wait(
            self.servicex.get_datasets(did_finder, show_deleted)
        )

    def get_dataset(self, dataset_id) -> CachedDataset:
        r"""
        Retrieve a dataset by its ID
        :return: A Query object
        """
        return _async_execute_and_wait(self.servicex.get_dataset(dataset_id))

    def delete_dataset(self, dataset_id) -> bool:
        r"""
        Delete a dataset by its ID
        :return: boolean showing whether the dataset has been deleted
        """
        return _async_execute_and_wait(self.servicex.delete_dataset(dataset_id))

    def delete_transform(self, transform_id) -> None:
        r"""
        Delete a Transform by its request ID
        """
        return _async_execute_and_wait(self.servicex.delete_transform(transform_id))

    def cancel_transform(self, transform_id) -> None:
        r"""
        Cancel a Transform by its request ID
        """
        return _async_execute_and_wait(self.servicex.cancel_transform(transform_id))

    def _ensure_code_generators(self) -> None:
        """Populate cached code generators if not already retrieved."""

        if self._code_generators is None:
            # Only hit the network the first time we need this information.
            self._code_generators = self.servicex.get_code_generators()

    def get_code_generators(self) -> dict[str, str]:
        r"""
        Retrieve the code generators deployed with the ServiceX instance.

        Returns the cached result if already fetched, otherwise performs a
        network request via :py:meth:`ServiceXAdapter.get_code_generators`.
        """
        self._ensure_code_generators()
        # _ensure_code_generators guarantees the attribute is populated
        return cast(dict[str, str], self._code_generators)

    def generic_query(
        self,
        dataset_identifier: DID,
        query: Union[str, QueryStringGenerator],
        codegen: Optional[str] = None,
        title: str = "ServiceX Client",
        result_format: ResultFormat = ResultFormat.parquet,
        ignore_cache: bool = False,
        fail_if_incomplete: bool = True,
    ) -> Query:
        r"""
        Generate a Query object for a generic codegen specification

        :param dataset_identifier:  The dataset identifier or filelist to be the source of files
        :param title: Title to be applied to the transform. This is also useful for
                      relating transform results.
        :param codegen: Name of the code generator to use with this transform
        :param result_format:  Do you want Paqrquet or Root? This can be set later with
                               the set_result_format method
        :param ignore_cache: Ignore the query cache and always run the query
        :return: A Query object

        """

        if query is None:
            raise ValueError("query argument cannot be None")

        if isinstance(query, str):
            if codegen is None:
                raise RuntimeError(
                    "A pure string query requires a codegen argument as well"
                )
            query = GenericQueryStringGenerator(query, codegen)
        if not isinstance(query, QueryStringGenerator):
            raise ValueError(
                "query argument must be string or QueryStringGenerator, not "
                f"{type(query).__name__}"
            )

        real_codegen = codegen if codegen is not None else query.default_codegen
        if real_codegen is None:
            raise RuntimeError(
                "No codegen specified, either from query class or user input"
            )

        qobj = Query(
            dataset_identifier=dataset_identifier,
            sx_adapter=self.servicex,
            title=title,
            codegen=real_codegen,
            config=self.config,
            query_cache=self.query_cache,
            result_format=result_format,
            ignore_cache=ignore_cache,
            query_string_generator=query,
            fail_if_incomplete=fail_if_incomplete,
        )
        return qobj

    def delete_transform_from_cache(self, transform_id: str):
        cache = self.query_cache
        rec = cache.get_transform_by_request_id(transform_id)
        if not rec:
            return False

        shutil.rmtree(rec.data_dir, ignore_errors=True)
        cache.delete_record_by_request_id(rec.request_id)
        return True
