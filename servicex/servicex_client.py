# Copyright (c) 2022, IRIS-HEP
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
from typing import Optional, List, TypeVar, Any, Mapping, Union, cast
from pathlib import Path

from servicex.configuration import Configuration
from servicex.models import ResultFormat, TransformStatus, TransformedResults
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
from collections.abc import Sequence
import traceback

T = TypeVar("T")
logger = logging.getLogger(__name__)


class ReturnValueException(Exception):
    """ An exception occurred at some point while obtaining this result from ServiceX """
    def __init__(self, exc):
        import copy
        message = ('Exception occurred while making ServiceX request.\n'
                   + (''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
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
            return f'Invalid GuardList: {repr(data._exc)}'


def _load_ServiceXSpec(
    config: Union[ServiceXSpec, Mapping[str, Any], str, Path]
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
        from ccorp.ruamel.yaml.include import YAML
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


def _build_datasets(config, config_path, servicex_name):
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
    datasets = []
    for sample in config.Sample:
        query = sx.generic_query(
            dataset_identifier=sample.dataset_identifier,
            title=sample.Name,
            codegen=get_codegen(sample, config.General),
            result_format=config.General.OutputFormat.to_ResultFormat(),
            ignore_cache=sample.IgnoreLocalCache,
            query=sample.Query,
        )
        logger.debug(f"Query string: {query.generate_selection_string()}")
        query.ignore_cache = sample.IgnoreLocalCache

        datasets.append(query)
    return datasets


def _output_handler(config: ServiceXSpec, requests: List[Query],
                    results: List[Union[TransformedResults, Exception]]):
    matched_results = zip(requests, results)
    if config.General.Delivery == General.DeliveryEnum.URLs:
        out_dict = {obj[0].title: GuardList(obj[1].signed_url_list
                                            if not isinstance(obj[1], Exception)
                                            else obj[1])
                    for obj in matched_results}
    elif config.General.Delivery == General.DeliveryEnum.LocalCache:
        out_dict = {obj[0].title: GuardList(obj[1].file_list
                                            if not isinstance(obj[1], Exception)
                                            else obj[1])
                    for obj in matched_results}

    if config.General.OutputDirectory:
        import yaml as yl

        out_dir = Path(config.General.OutputDirectory).absolute()
        out_dir.mkdir(parents=True, exist_ok=True)
        out_dict_path = Path(out_dir, config.General.OutFilesetName + ".yaml")

        with open(out_dict_path, "w") as f:
            yl.dump(out_dict, f, default_flow_style=False)

    return out_dict


def deliver(
    config: Union[ServiceXSpec, Mapping[str, Any], str, Path],
    config_path: Optional[str] = None,
    servicex_name: Optional[str] = None,
    return_exceptions: bool = True
):
    config = _load_ServiceXSpec(config)

    datasets = _build_datasets(config, config_path, servicex_name)

    group = DatasetGroup(datasets)

    if config.General.Delivery == General.DeliveryEnum.URLs:
        results = group.as_signed_urls(return_exceptions=return_exceptions)
        return _output_handler(config, datasets, results)

    elif config.General.Delivery == General.DeliveryEnum.LocalCache:
        results = group.as_files(return_exceptions=return_exceptions)
        return _output_handler(config, datasets, results)


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
        :param config_path: Optional path te the `.servicex` file. If not specified,
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
                raise ValueError(f"Backend {backend} not defined in .servicex file")
            self.servicex = ServiceXAdapter(
                self.endpoints[backend].endpoint,
                refresh_token=self.endpoints[backend].token,
            )

        self.query_cache = QueryCache(self.config)
        self.code_generators = set(self.get_code_generators(backend).keys())

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

    def get_code_generators(self, backend=None):
        r"""
        Retrieve the code generators deployed with the serviceX instance
        :return:  The list of code generators as json dictionary
        """
        cached_backends = None
        if backend:
            cached_backends = self.query_cache.get_codegen_by_backend(backend)
        if cached_backends:
            logger.info("Returning code generators from cache")
            return cached_backends["codegens"]
        else:
            code_generators = self.servicex.get_code_generators()
            self.query_cache.update_codegen_by_backend(backend, code_generators)
            return code_generators

    def generic_query(
        self,
        dataset_identifier: DID,
        query: Union[str, QueryStringGenerator],
        codegen: Optional[str] = None,
        title: str = "ServiceX Client",
        result_format: ResultFormat = ResultFormat.parquet,
        ignore_cache: bool = False,
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

        if isinstance(query, str):
            if codegen is None:
                raise RuntimeError("A pure string query requires a codegen argument as well")
            query = GenericQueryStringGenerator(query, codegen)
        if not isinstance(query, QueryStringGenerator):
            raise ValueError("query argument must be string or QueryStringGenerator")

        real_codegen = codegen if codegen is not None else query.default_codegen
        if real_codegen is None:
            raise RuntimeError(
                "No codegen specified, either from query class or user input"
            )

        if real_codegen not in self.code_generators:
            raise NameError(
                f"{codegen} code generator not supported by serviceX "
                f"deployment at {self.servicex.url}"
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
            query_string_generator=query
        )
        return qobj
