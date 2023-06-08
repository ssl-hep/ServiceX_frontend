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
import abc
import asyncio
import os.path
from abc import ABC
from asyncio import Task, CancelledError

from servicex_client.types import DID

try:
    import pandas
except ModuleNotFoundError:
    pass

import rich
from rich.progress import Progress, TaskID, TextColumn, BarColumn, MofNCompleteColumn, \
    TimeRemainingColumn

from servicex_client.configuration import Configuration
from servicex_client.minio_adpater import MinioAdapter
from servicex_client.models import TransformRequest, ResultDestination, ResultFormat, \
    Status
from servicex_client.query_cache import QueryCache
from servicex_client.servicex_adapter import ServiceXAdapter


class Dataset(ABC):

    def __init__(
            self,
            dataset_identifier: DID = None,
            title: str = "ServiceX Client",
            codegen: str = None,
            sx_adapter: ServiceXAdapter = None,
            config: Configuration = None,
            query_cache: QueryCache = None,
            servicex_polling_interval: int = 10,
            minio_polling_interval: int = 5):
        super(Dataset, self).__init__()
        self.servicex = sx_adapter
        self.configuration = config
        self.cache = query_cache

        self.dataset_identifier = dataset_identifier
        self.codegen = codegen
        self.title = title

        self.result_format = None
        self.signed_urls = False
        self.current_status = None
        self.download_path = None
        self.minio = None
        self.files_failed = None
        self.files_completed = None
        self._return_qastle = True

        self.request_id = None

        # Number of seconds in between ServiceX status polls
        self.servicex_polling_interval = servicex_polling_interval
        self.minio_polling_interval = minio_polling_interval

    @abc.abstractmethod
    def generate_selection_string(self) -> str:
        pass

    @property
    def transform_request(self):
        if not self.result_format:
            raise ValueError("Unable to determine the result file format. Use set_result_format method")  # NOQA E501

        sx_request = TransformRequest(
            title=self.title,
            codegen=self.codegen,
            result_destination=ResultDestination.object_store,
            result_format=self.result_format,
            selection=self.generate_selection_string()
        )
        # Transfer the DID into the transform request
        self.dataset_identifier.populate_transform_request(sx_request)
        return sx_request

    def set_result_format(self, result_format: ResultFormat):
        self.result_format = result_format
        return self

    async def submit_and_download(self, signed_urls_only: bool = False):
        """
        Submit the transform request to ServiceX. Poll the transform status to see when
        the transform completes and to get the number of files in the dataset along with
        current progress and failed file count.
        :return:
        """
        download_files_task = None
        loop = asyncio.get_running_loop()

        def transform_complete(task: Task):
            """
            Called when the Monitor task completes. This could be because of exception or
            the transform completed
            :param task:
            :return:
            """
            if task.exception():
                rich.print("ServiceX Exception", task.exception())
                if download_files_task:
                    download_files_task.cancel("Transform failed")
                raise task.exception()

            if self.current_status.files_failed:
                rich.print(f"[bold red]Transforms completed with failures[/bold red] "
                           f"{self.current_status.files_failed} files failed out of "
                           f"{self.current_status.files}")
            else:
                rich.print("Transforms completed successfully")

        sx_request = self.transform_request

        # Let's see if this is in the cache already
        cached_record = self.cache.get_transform_by_hash(sx_request.compute_hash())

        if cached_record:
            rich.print("Returning results from cache")
            return cached_record.file_uris

        with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(compact=True, elapsed_when_finished=True),
        ) as progress:
            transform_progress = progress.add_task("Transform", start=False, total=None)

            minio_progress_bar_title = "Download" \
                if not signed_urls_only else "Signing URLS"

            download_progress = progress.add_task(minio_progress_bar_title,
                                                  start=False, total=None)

            self.request_id = await self.servicex.submit_transform(sx_request)

            monitor_task = loop.create_task(
                self.transform_status_listener(progress,
                                               transform_progress,
                                               download_progress))
            monitor_task.add_done_callback(transform_complete)

            download_files_task = loop.create_task(
                self.download_files(signed_urls_only,
                                    progress, download_progress))

            try:
                downloaded_files = await download_files_task

                # Update the cache
                self.cache.cache_transform(sx_request, self.current_status,
                                           self.download_path.as_posix(),
                                           downloaded_files)
                return downloaded_files
            except CancelledError:
                rich.print_json("Aborted file downloads due to transform failure")

    async def transform_status_listener(self, progress: Progress,
                                        progress_task: TaskID, download_task: TaskID):
        """
        Poll ServiceX for the status of a transform. Update progress bars and keep track
        of status. Once we know the number of files in the dataset, update the progress
        bars.
        """

        # Actual number of files in the dataset. We only know this once the DID
        # finder has completed its work. In the meantime transformers will already
        # start up and begin work on the files we know about
        final_count = None

        while True:
            s = await self.servicex.get_transform_status(self.request_id)

            # Is this the first time we've polled status? We now know the request ID.
            # Update the display and set our download directory.
            if not self.current_status:
                rich.print(f"[bold]ServiceX Transform {s.request_id}[/bold]")
                self.download_path = self.cache.cache_path_for_transform(s)

            self.current_status = s

            # Do we finally know the final number of files in the dataset? Now is the
            # time to properly initialize the progress bars
            if not final_count and self.current_status.files:
                final_count = self.current_status.files
                progress.update(progress_task, total=final_count)
                progress.start_task(progress_task)

                progress.update(download_task, total=final_count)
                progress.start_task(download_task)

            progress.update(progress_task, completed=self.current_status.files_completed)

            # We can only initialize the minio adapter with data from the transform
            # status. This includes the minio host and credentials. We use the
            # transform id as the bucket.
            if not self.minio:
                self.minio = MinioAdapter.for_transform(self.current_status)

            if self.current_status.status == Status.complete:
                self.files_completed = self.current_status.files_completed
                self.files_failed = self.current_status.files_failed
                return

            await asyncio.sleep(self.servicex_polling_interval)

    async def download_files(self, signed_urls_only: bool,
                             progress: Progress,
                             download_progress: TaskID):
        """
        Task to monitor the list of files in the transform output's bucket. Any new files
        will be downloaded.
        """
        files_seen = set()
        downloaded_file_paths = []
        download_tasks = []
        loop = asyncio.get_running_loop()

        async def download_file(minio: MinioAdapter, filename: str,
                                progress: Progress,
                                download_progress: TaskID):
            await minio.download_file(filename, self.download_path)
            downloaded_file_paths.append(os.path.join(self.download_path, filename))
            progress.advance(download_progress)

        async def get_signed_url(minio: MinioAdapter, filename: str,
                                 progress: Progress,
                                 download_progress: TaskID):
            url = await minio.get_signed_url(filename)
            downloaded_file_paths.append(url)
            progress.advance(download_progress)

        while True:
            await asyncio.sleep(self.minio_polling_interval)
            if self.minio:
                files = await self.minio.list_bucket()
                for file in files:
                    if file.filename not in files_seen:
                        if signed_urls_only:
                            download_tasks.append(
                                loop.create_task(
                                    get_signed_url(self.minio, file.filename,
                                                   progress, download_progress))
                            )
                        else:
                            download_tasks.append(
                                loop.create_task(
                                    download_file(self.minio, file.filename,
                                                  progress, download_progress)))
                        files_seen.add(file.filename)

            # Once the transform is complete we can stop polling since all of the files
            # are guaranteed to be in the bucket.
            if self.current_status and self.current_status.status == Status.complete:
                break

        # Now just wait until all of our tasks complete
        await asyncio.gather(*download_tasks)
        return downloaded_file_paths

    async def as_parquet_files(self):
        self.result_format = ResultFormat.parquet
        return await self.submit_and_download()

    async def as_root_files(self):
        self.result_format = ResultFormat.root_file
        return await self.submit_and_download()

    async def as_pandas(self):
        parquet_files = await self.as_parquet_files()
        dataframes = [pandas.read_parquet(p) for p in parquet_files]
        return dataframes

    async def as_signed_urls(self):
        return await self.submit_and_download(signed_urls_only=True)
