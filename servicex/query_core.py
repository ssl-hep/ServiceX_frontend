# Copyright (c) 2024-2025, IRIS-HEP
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
from __future__ import annotations

import datetime
import abc
import asyncio
from abc import ABC
from asyncio import Task, CancelledError
import logging
from typing import List, Optional, Union
from servicex.expandable_progress import ExpandableProgress
from rich.logging import RichHandler

from servicex.types import DID

from rich.progress import Progress, TaskID

from servicex.configuration import Configuration
from servicex.minio_adapter import MinioAdapter
from servicex.models import (
    TransformRequest,
    ResultDestination,
    ResultFormat,
    Status,
    TransformedResults,
)
from servicex.query_cache import QueryCache
from servicex.servicex_adapter import ServiceXAdapter

from make_it_sync import make_sync

DONE_STATUS = (Status.complete, Status.canceled, Status.fatal)
ProgressIndicators = Union[Progress, ExpandableProgress]
logger = logging.getLogger(__name__)
shell_handler = RichHandler(markup=True)
logger.addHandler(shell_handler)


class ServiceXException(Exception):
    """Something happened while trying to carry out a ServiceX request"""


class Query:
    def __init__(
        self,
        dataset_identifier: DID,
        title: str,
        codegen: str,
        sx_adapter: ServiceXAdapter,
        config: Configuration,
        query_cache: Optional[QueryCache],
        servicex_polling_interval: int = 5,
        minio_polling_interval: int = 5,
        result_format: ResultFormat = ResultFormat.parquet,
        ignore_cache: bool = False,
        query_string_generator: Optional[QueryStringGenerator] = None,
        fail_if_incomplete: bool = True,
    ):
        r"""
        This is the main class for constructing transform requests and receiving the
        results in a format of your choice. It can be run synchronously or asynchronously.

        :param dataset_identifier: Either a Rucio DID or a list of files
        :param title: Human readable title for this transform
        :param codegen: Name of the code generator
        :param sx_adapter:
        :param config:
        :param query_cache:
        :param servicex_polling_interval: How many seconds between polling for
                                          transform status?
        :param minio_polling_interval:  How many seconds between polling the minio bucket
                                        for new files?
        :param result_format:
        :param ignore_cache:  If true, ignore the cache and always submit a new transform
        :param fail_if_incomplete: If true, raise an exception if we don't have 100% completion
        """
        self.servicex = sx_adapter
        self.configuration = config
        self.cache = query_cache

        self.dataset_identifier = dataset_identifier
        self.codegen = codegen
        self.title = title

        self.result_format = result_format
        self.signed_urls = False
        self.current_status = None
        self.download_path = None
        self.minio = None
        self.files_failed = None
        self.files_completed = None
        self._return_qastle = True

        self.request_id = None
        self.ignore_cache = ignore_cache
        self.fail_if_incomplete = fail_if_incomplete
        self.query_string_generator = query_string_generator

        # Number of seconds in between ServiceX status polls
        self.servicex_polling_interval = servicex_polling_interval
        self.minio_polling_interval = minio_polling_interval

    def generate_selection_string(self) -> str:
        if self.query_string_generator is None:
            raise RuntimeError("query string generator not set")
        return self.query_string_generator.generate_selection_string()

    @property
    def transform_request(self):
        if not self.result_format:
            raise ValueError(
                "Unable to determine the result file format. Use set_result_format method"
            )  # NOQA E501

        sx_request = TransformRequest(
            title=self.title,
            codegen=self.codegen,
            result_destination=ResultDestination.object_store,  # type: ignore
            result_format=self.result_format,  # type: ignore
            selection=self.generate_selection_string(),
        )  # type: ignore
        # Transfer the DID into the transform request
        self.dataset_identifier.populate_transform_request(sx_request)
        return sx_request

    def set_title(self, title: str) -> Query:
        self.title = title
        return self

    def set_result_format(self, result_format: ResultFormat):
        r"""
        Set the result format - required at constructor time or as part of the query
        chain of methods
        :param result_format:
        :return: self to allow you to chain together query and setup methods
        """
        self.result_format = result_format
        return self

    async def submit_and_download(
        self,
        signed_urls_only: bool,
        expandable_progress: ExpandableProgress,
        dataset_group: Optional[bool] = False,
    ) -> Optional[TransformedResults]:
        """
        Submit the transform request to ServiceX. Poll the transform status to see when
        the transform completes and to get the number of files in the dataset along with
        current progress and failed file count.

        :param signed_urls_only: Set to true to skip actually downloading the files and
                                 just return pre-signed urls
        :param display_progress: Set to false to disable the progress bar
        :param expandable_progress: Provide an existing progress bar. Set to None to have
                                    one created for you

        :return: Transform results object which contains the list of files downloaded
                 or the list of pre-signed urls
        """
        from servicex.app.transforms import (
            create_kibana_link_parameters,
            TimeFrame,
            LogLevel,
        )

        download_files_task = None
        loop = asyncio.get_running_loop()

        def transform_complete(task: Task):
            """
            Called when the Monitor task completes. This could be because of exception or
            the transform completed
            :param task:
            :return:
            """
            expandable_progress.refresh()
            if task.exception():
                logger.error(
                    f'ServiceX Exception for request ID {self.request_id} ({self.title})"',
                    exc_info=task.exception(),
                )
                if self.fail_if_incomplete:
                    self.cache.delete_record_by_request_id(self.request_id)
                    if download_files_task:
                        download_files_task.cancel("Transform failed")
                raise task.exception()

            if self.current_status.status in DONE_STATUS:
                if self.current_status.files_failed:
                    titlestr = (
                        f'"{self.current_status.title}" '
                        if self.current_status.title is not None
                        else ""
                    )
                    errorstr = (
                        f"Transform {titlestr}completed with failures: "
                        f"{self.current_status.files_failed}/"
                        f"{self.current_status.files} files failed."
                        f"{'Will not cache.' if self.fail_if_incomplete else ''}"
                    )
                    failedfiles = (
                        self.servicex.url
                        + "/transformation-request/"
                        + f"/{self.request_id}/results?status=failure"
                    )
                    errorstr2 = (
                        "A list of failed files is at [bold red on white]"
                        f"[link={failedfiles}]this link[/link][/bold red on white]"
                    )
                    logger.error(errorstr)
                    logger.error(errorstr2)
                    logger.error(
                        f"Transform Request id: {self.current_status.request_id}"
                    )
                    if self.current_status.log_url is not None:
                        kibana_link = create_kibana_link_parameters(
                            self.current_status.log_url,
                            self.current_status.request_id,
                            LogLevel.error,
                            TimeFrame.month,
                        )
                        logger.error(
                            f"More information of '{self.title}' [bold red on white][link={kibana_link}]HERE[/link][/bold red on white]"  # NOQA: E501
                        )
                    if self.fail_if_incomplete:
                        self.cache.delete_record_by_request_id(self.request_id)
                        raise ServiceXException(errorstr)
                    else:
                        logger.error("Will continue to download what is available")
                else:
                    logger.info("Transforms completed successfully")
            else:  # pragma: no cover
                logger.info(
                    f"Transforms finished with code {self.current_status.status}"
                )

        sx_request = self.transform_request
        sx_request_hash = sx_request.compute_hash()

        # Invalidate the cache if the hash already present but if the user ignores cache
        if self.ignore_cache and (
            self.cache.contains_hash(sx_request_hash)
            or self.cache.is_transform_request_submitted(sx_request_hash)
        ):
            self.cache.delete_record_by_hash(sx_request_hash)

        # Let's see if this is in the cache already, but respect the user's wishes
        # to ignore the cache
        cached_record = (
            self.cache.get_transform_by_hash(sx_request_hash)
            if not self.ignore_cache
            else None
        )

        # And that we grabbed the resulting files in the way that the user requested
        # (Downloaded, or obtained pre-signed URLs)
        if cached_record:
            if (signed_urls_only and cached_record.signed_url_list) or (
                not signed_urls_only and cached_record.file_list
            ):
                logger.info("Returning results from cache")
                return cached_record

        # If we get here with a cached record, then we know that the transform
        # has been run, but we just didn't get the files from object store in the way
        # requested by user
        transform_bar_title = f"{sx_request.title}: Transform"
        if not cached_record:
            transform_progress = (
                expandable_progress.add_task(
                    transform_bar_title, start=False, total=None
                )
                if expandable_progress
                else None
            )
        else:
            self.request_id = cached_record.request_id
            transform_progress = None
            await self.retrieve_current_transform_status()

        minio_progress_bar_title = (
            "Download" if not signed_urls_only else "Signing URLS"
        )
        minio_progress_bar_title = minio_progress_bar_title.rjust(
            len(transform_bar_title)
        )

        download_progress = (
            expandable_progress.add_task(
                minio_progress_bar_title, start=False, total=None
            )
            if expandable_progress
            else None
        )

        if not cached_record:
            # Validate the requested code generator only when we are about to
            # submit a new transform. This avoids a network call when a cached
            # transform is used.
            supported_codegens = await self.servicex.get_code_generators_async()
            if self.codegen not in supported_codegens:
                # Include available code generators to guide user when an
                # unsupported one is requested.
                available_codegens = ", ".join(sorted(supported_codegens))
                raise NameError(
                    f"{self.codegen} code generator not supported by serviceX "
                    f"deployment at {self.servicex.url}. Supported code generators are: "
                    f"{available_codegens}"
                )

            if self.cache.is_transform_request_submitted(sx_request_hash):
                self.request_id = self.cache.get_transform_request_id(sx_request_hash)
            else:
                self.request_id = await self.servicex.submit_transform(sx_request)
                self.cache.cache_submitted_transform(sx_request, self.request_id)

            monitor_task = loop.create_task(
                self.transform_status_listener(
                    expandable_progress,
                    transform_progress,
                    transform_bar_title,
                    download_progress,
                    minio_progress_bar_title,
                )
            )
            monitor_task.add_done_callback(transform_complete)
        else:
            self.request_id = cached_record.request_id

        download_files_task = loop.create_task(
            self.download_files(
                signed_urls_only,
                expandable_progress,
                download_progress,
                cached_record,
            )
        )

        try:
            signed_urls = []
            downloaded_files = []

            download_result = await download_files_task
            if signed_urls_only:
                signed_urls = download_result
                if cached_record:
                    cached_record.signed_url_list = download_result
            else:
                downloaded_files = download_result
                if cached_record:
                    cached_record.file_list = download_result

            # Update the cache (if no failed files)
            if not cached_record:
                transform_report = self.cache.transformed_results(
                    sx_request,
                    self.current_status,
                    self.download_path.as_posix(),
                    downloaded_files,
                    signed_urls,
                )
                if self.current_status.files_failed == 0:
                    self.cache.update_transform_status(sx_request_hash, "COMPLETE")
                    self.cache.cache_transform(transform_report)
            else:
                if self.current_status.files_failed == 0:
                    self.cache.update_record(cached_record)
                transform_report = cached_record

            return transform_report
        except CancelledError:
            logger.warning("Aborted file downloads due to transform failure")

        _ = await monitor_task  # raise exception, if it is there

    async def transform_status_listener(
        self,
        progress: ExpandableProgress,
        progress_task: TaskID,
        progress_bar_title: str,
        download_task: TaskID,
        download_bar_title: str,
    ):
        """
        Poll ServiceX for the status of a transform. Update progress bars and keep track
        of status. Once we know the number of files in the dataset, update the progress
        bars.
        """
        from servicex.app.transforms import (
            LogLevel,
            create_kibana_link_parameters,
            TimeFrame,
        )

        # Actual number of files in the dataset. We only know this once the DID
        # finder has completed its work. In the meantime transformers will already
        # start up and begin work on the files we know about
        final_count = None

        while True:
            await self.retrieve_current_transform_status()

            # Do we finally know the final number of files in the dataset? Now is the
            # time to properly initialize the progress bars
            if not final_count and self.current_status.files:
                final_count = self.current_status.files
                if progress:
                    progress.update(
                        progress_task, progress_bar_title, total=final_count
                    )
                    progress.start_task(task_id=progress_task, task_type="Transform")

                    progress.update(
                        download_task, download_bar_title, total=final_count
                    )
                    progress.start_task(task_id=download_task, task_type="Download")

            if progress:
                # update the transform progress bar to get the total number of files
                progress.update(
                    progress_task,
                    progress_bar_title,
                    total=self.current_status.files,
                    completed=self.current_status.files_completed,
                )

                # update the download progress bar to get the total number of files
                progress.update(
                    download_task, download_bar_title, total=self.current_status.files
                )

            if self.current_status.status in DONE_STATUS:
                self.files_completed = self.current_status.files_completed
                self.files_failed = self.current_status.files_failed
                titlestr = (
                    f'"{self.current_status.title}" '
                    if self.current_status.title is not None
                    else ""
                )
                if self.current_status.status == Status.complete:
                    if self.files_failed:
                        bar = "failure"
                    else:
                        bar = "complete"
                    progress.update(
                        progress_task,
                        progress_bar_title,
                        self.current_status.files,
                        completed=self.current_status.files_completed,
                        bar=bar,
                    )
                    return
                elif self.current_status.status == Status.canceled:
                    logger.warning(
                        f"Request {titlestr}canceled: "
                        f"{self.current_status.files_completed}/{self.current_status.files} "
                        f"files completed"
                    )
                    err_str = f"Request {titlestr}was canceled"
                    if self.current_status.log_url is not None:
                        kibana_link = create_kibana_link_parameters(
                            self.current_status.log_url,
                            self.current_status.request_id,
                            LogLevel.error,
                            TimeFrame.month,
                        )
                        logger.error(
                            f"{err_str}\nMore logfiles of '{self.title}' [bold red on white][link={kibana_link}]HERE[/link][/bold red on white]"  # NOQA: E501
                        )
                    raise ServiceXException(err_str)

                else:
                    err_str = f"Fatal issue in ServiceX server for request {titlestr}"
                    if self.current_status.log_url is not None:
                        kibana_link = create_kibana_link_parameters(
                            self.current_status.log_url,
                            self.current_status.request_id,
                            LogLevel.error,
                            TimeFrame.month,
                        )
                        logger.error(
                            f"{err_str}\nMore logfiles of '{self.title}' [bold red on white][link={kibana_link}]HERE[/link][/bold red on white]"  # NOQA: E501
                        )
                    raise ServiceXException(err_str)

            await asyncio.sleep(self.servicex_polling_interval)

    async def retrieve_current_transform_status(self):
        s = await self.servicex.get_transform_status(self.request_id)

        # Is this the first time we've polled status? We now know the request ID.
        # Update the display and set our download directory.
        if not self.current_status:
            logger.info(f"ServiceX Transform {s.title}: {s.request_id}")
            self.download_path = self.cache.cache_path_for_transform(s)

        self.current_status = s

        # We can only initialize the minio adapter with data from the transform
        # status. This includes the minio host and credentials. We use the
        # transform id as the bucket.
        if not self.minio:
            self.minio = MinioAdapter.for_transform(self.current_status)

    async def download_files(
        self,
        signed_urls_only: bool,
        progress: ExpandableProgress,
        download_progress: TaskID,
        cached_record: Optional[TransformedResults],
    ) -> List[str]:
        """
        Task to monitor the list of files in the transform output's bucket. Any new files
        will be downloaded.
        """

        files_seen = set()
        result_uris = []
        download_tasks = []
        loop = asyncio.get_running_loop()

        async def download_file(
            minio: MinioAdapter,
            filename: str,
            progress: Progress,
            download_progress: TaskID,
            shorten_filename: bool = False,
            expected_size: Optional[int] = None,
        ):
            downloaded_filename = await minio.download_file(
                filename,
                self.download_path,
                shorten_filename=shorten_filename,
                expected_size=expected_size,
            )
            result_uris.append(downloaded_filename.as_posix())
            progress.advance(task_id=download_progress, task_type="Download")

        async def get_signed_url(
            minio: MinioAdapter,
            filename: str,
            progress: Optional[Progress],
            download_progress: TaskID,
        ):
            url = await minio.get_signed_url(filename)
            result_uris.append(url)
            if progress:
                progress.advance(task_id=download_progress, task_type="Download")

        later_than = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

        use_local_polling = (
            "poll_local_transformation_results"
            in await self.servicex.get_servicex_capabilities()
        )

        if not use_local_polling:
            logger.warning(
                "ServiceX is using legacy S3 bucket polling. Future versions of the "
                "ServiceX client will not support this method. Please update your "
                "ServiceX server to the latest version."
            )

        while True:
            if not cached_record:
                await asyncio.sleep(self.minio_polling_interval)
            if self.minio:
                # if self.minio exists, self.current_status will too
                if self.current_status.files_completed > len(files_seen):
                    if use_local_polling:
                        files = await self.servicex.get_transformation_results(
                            self.current_status.request_id, later_than
                        )
                    else:
                        files = await self.minio.list_bucket()

                    for file in files:
                        filename = file.filename

                        if filename != "" and filename not in files_seen:
                            if signed_urls_only:
                                download_tasks.append(
                                    loop.create_task(
                                        get_signed_url(
                                            self.minio,
                                            filename,
                                            progress,
                                            download_progress,
                                        )
                                    )
                                )
                            else:
                                if use_local_polling:
                                    expected_size = file.total_bytes
                                else:
                                    expected_size = file.size
                                download_tasks.append(
                                    loop.create_task(
                                        download_file(
                                            self.minio,
                                            filename,
                                            progress,
                                            download_progress,
                                            shorten_filename=self.configuration.shortened_downloaded_filename,  # NOQA: E501
                                            expected_size=expected_size,
                                        )
                                    )
                                )  # NOQA 501
                            files_seen.add(filename)

                            if use_local_polling:
                                if file.created_at > later_than:
                                    later_than = file.created_at

            # Once the transform is complete and all files are seen we can stop polling.
            # Also, if we are just downloading or signing urls for a previous transform
            # then we know it is complete as well
            if cached_record or (
                self.current_status
                and (
                    self.current_status.status in DONE_STATUS
                    and self.current_status.files_completed == len(files_seen)
                )
            ):
                break

        # Now just wait until all of our tasks complete
        await asyncio.gather(*download_tasks)
        return result_uris

    async def as_files_async(
        self,
        display_progress: bool = True,
        provided_progress: Optional[ProgressIndicators] = None,
    ) -> TransformedResults:
        r"""
        Submit the transform and request all the resulting files to be downloaded
        :return: TransformResult instance with the list of complete paths to the downloaded files
        """
        with ExpandableProgress(display_progress, provided_progress) as progress:
            return await self.submit_and_download(
                signed_urls_only=False, expandable_progress=progress
            )

    as_files = make_sync(as_files_async)

    async def as_signed_urls_async(
        self,
        display_progress: bool = True,
        provided_progress: Optional[ProgressIndicators] = None,
        dataset_group: bool = False,
    ) -> TransformedResults:
        r"""
        Presign URLs for each of the transformed files

        :return: TransformedResults object with the presigned_urls list populated
        """
        if dataset_group:
            return await self.submit_and_download(
                signed_urls_only=True,
                expandable_progress=provided_progress,
                dataset_group=dataset_group,
            )

        with ExpandableProgress(
            display_progress=display_progress, provided_progress=provided_progress
        ) as progress:
            return await self.submit_and_download(
                signed_urls_only=True,
                expandable_progress=progress,
                dataset_group=dataset_group,
            )

    as_signed_urls = make_sync(as_signed_urls_async)


class QueryStringGenerator(ABC):
    """This abstract class just defines an interface to give the selection string"""

    @abc.abstractmethod
    def generate_selection_string(self) -> str:
        """override with the selection string to send to ServiceX"""

    """ override with the codegen string you would like associated with this query class """
    default_codegen: Optional[str] = None


class GenericQueryStringGenerator(QueryStringGenerator):
    """Return the string from the initializer"""

    def __init__(self, query: str, codegen: str):
        self.query = query
        self.default_codegen = codegen

    def generate_selection_string(self) -> str:
        return self.query
