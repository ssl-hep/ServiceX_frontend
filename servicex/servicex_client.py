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
from typing import Optional, List, TypeVar, Any, Type

from servicex.configuration import Configuration
from servicex.func_adl.func_adl_dataset import FuncADLQuery
from servicex.models import ResultFormat, TransformStatus
from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter
from servicex.types import DID
from servicex.python_dataset import PythonQuery
from servicex.dataset_group import DatasetGroup
import ast
import qastle

from make_it_sync import make_sync
from servicex.databinder_models import ServiceXSpec, General, Sample

T = TypeVar("T")
logger = logging.getLogger(__name__)


def deliver(config: ServiceXSpec):
    def get_codegen(_sample: Sample, _general: General):
        if _sample.Codegen:
            return _sample.Codegen
        else:
            return _general.Codegen

    sx = ServiceXClient(backend=config.General.ServiceX)
    datasets = []
    for sample in config.Sample:
        if sample.Query:
            if type(sample.Query) is str:
                qastle_query = qastle.python_ast_to_text_ast(
                    ast.parse(sample.Query)
                )  # NOQA E501
                sample.Query = FuncADLQuery()

                sample.Query.set_provided_qastle(qastle_query)

            query = sx.func_adl_dataset(
                sample.dataset_identifier,
                sample.Name,
                get_codegen(sample, config.General),
                config.General.OutputFormat,
            )
            query._q_ast = sample.Query._q_ast
            query._item_type = sample.Query._item_type
            if sample.Tree:
                query = query.set_tree(sample.Tree)

            sample.Query = query
            sample.Query.ignore_cache = sample.IgnoreLocalCache

            datasets.append(sample.Query)
        elif sample.Function:
            # The function field can be a callable if this was all initialized
            # in python, or it can be a string if it was initialized from a
            # yaml file. If it comes from a string, let's validate the syntax
            # now to avoid nasty surprises later.
            if isinstance(sample.Function, str):
                try:
                    exec(sample.Function)
                except SyntaxError as e:
                    raise SyntaxError(f"Syntax error in {sample.Name}: {e}")

            dataset = sx.python_dataset(
                sample.dataset_identifier,
                sample.Name,
                get_codegen(sample, config.General),
                config.General.OutputFormat,
            )
            dataset.python_function = sample.Function
            dataset.ignore_cache = sample.IgnoreLocalCache
            datasets.append(dataset)

    group = DatasetGroup(datasets)

    if config.General.Delivery == General.DeliveryEnum.SignedURLs:
        results = group.as_signed_urls()
        return {obj.title: obj.signed_url_list for obj in results}

    elif config.General.Delivery == General.DeliveryEnum.LocalCache:
        results = group.as_files()
        return {obj.title: obj.file_list for obj in results}


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
            backend = self.config.default_endpoint

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

    def func_adl_dataset(
        self,
        dataset_identifier: DID,
        title: str = "ServiceX Client",
        codegen: str = "uproot",
        result_format: Optional[ResultFormat] = None,
        item_type: Type[T] = Any,
        ignore_cache: bool = False,
    ) -> FuncADLQuery[T]:
        r"""
        Generate a dataset that can use func_adl query language

        :param dataset_identifier:  The dataset identifier or filelist to be the source of files
        :param title: Title to be applied to the transform. This is also useful for
                      relating transform results.
        :param codegen: Name of the code generator to use with this transform
        :param result_format:  Do you want Paqrquet or Root? This can be set later with
                               the set_result_format method
        :param item_type: The type of the items that will be returned from the query
        :param ignore_cache: Ignore the query cache and always run the query
        :return: A func_adl dataset ready to accept query statements.
        """
        if codegen not in self.code_generators:
            raise NameError(
                f"{codegen} code generator not supported by serviceX "
                f"deployment at {self.servicex.url}"
            )

        return FuncADLQuery(
            dataset_identifier,
            sx_adapter=self.servicex,
            title=title,
            codegen=codegen,
            config=self.config,
            query_cache=self.query_cache,
            result_format=result_format,
            item_type=item_type,
            ignore_cache=ignore_cache,
        )

    def python_dataset(
        self,
        dataset_identifier: DID,
        title: str = "ServiceX Client",
        codegen: str = "uproot",
        result_format: Optional[ResultFormat] = None,
        ignore_cache: bool = False,
    ) -> PythonQuery:
        r"""
        Generate a dataset that can use accept a python function for the  query

        :param dataset_identifier:  The dataset identifier or filelist to be the source of files
        :param title: Title to be applied to the transform. This is also useful for
                      relating transform results.
        :param codegen: Name of the code generator to use with this transform
        :param result_format:  Do you want Paqrquet or Root? This can be set later with
                               the set_result_format method
        :param ignore_cache: Ignore the query cache and always run the query
        :return: A func_adl dataset ready to accept a python function statements.

        """

        if codegen not in self.code_generators:
            raise NameError(
                f"{codegen} code generator not supported by serviceX "
                f"deployment at {self.servicex.url}"
            )

        return PythonQuery(
            dataset_identifier,
            sx_adapter=self.servicex,
            title=title,
            codegen=codegen,
            config=self.config,
            query_cache=self.query_cache,
            result_format=result_format,
            ignore_cache=ignore_cache,
        )
