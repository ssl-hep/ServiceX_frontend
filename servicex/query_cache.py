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
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from filelock import FileLock
from tinydb import TinyDB, Query, where
from tinydb.storages import JSONStorage

from servicex.configuration import Configuration
from servicex.models import TransformRequest, TransformStatus, TransformedResults


class CacheException(Exception):
    pass


class _MemoizedJSONStorage(JSONStorage):
    """
    A TinyDB JSON storage that only parses the database file when it has changed.

    TinyDB parses the whole file on every read. With a large cache and hundreds of
    samples, those parses block the event loop before any transform is submitted.
    Other processes can write to the same file, so the parsed copy is only reused
    while the file's modification time and size are unchanged.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._memo: Optional[Dict[str, Dict[str, Any]]] = None
        self._signature: Optional[Tuple[int, int]] = None

    def _current_signature(self) -> Tuple[int, int]:
        st = os.fstat(self._handle.fileno())
        return (st.st_mtime_ns, st.st_size)

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        signature = self._current_signature()
        if self._memo is None or signature != self._signature:
            self._memo = super().read()
            self._signature = signature
        return self._memo

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        # TinyDB changes the parsed data in place before writing it, so the memo must
        # not survive a failed write.
        self._memo = None
        super().write(data)
        self._memo = data
        self._signature = self._current_signature()

    def close(self) -> None:
        # Don't keep the parsed database in memory after the storage is closed.
        self._memo = None
        super().close()


class QueryCache:
    def __init__(self, config: Configuration):
        self.config = config
        if self.config.cache_path is not None:
            Path(self.config.cache_path).mkdir(parents=True, exist_ok=True)
            Path(self.config.cache_path + "/.servicex").mkdir(
                parents=True, exist_ok=True
            )
            self.db = TinyDB(
                os.path.join(self.config.cache_path, ".servicex", "db.json"),
                storage=_MemoizedJSONStorage,
            )
            # TinyDB's query cache only notices writes from this process, so it can
            # return stale results when another process changes the database.
            self.db.table(self.db.default_table_name, cache_size=0)
            self.lock = FileLock(
                os.path.join(self.config.cache_path, ".servicex", "db.lock")
            )

    def close(self):
        self.db.close()

    def transformed_results(
        self,
        transform: TransformRequest,
        completed_status: TransformStatus,
        data_dir: str,
        file_list: List[str],
        signed_urls: List[str],
        headers: List[dict[str, str]],
        expiries: List[int],
    ) -> TransformedResults:
        return TransformedResults(
            hash=transform.compute_hash(),
            title=transform.title or "No Title",
            codegen=transform.codegen,
            request_id=completed_status.request_id,
            submit_time=completed_status.submit_time,
            data_dir=data_dir,
            file_list=file_list,
            signed_url_list=signed_urls,
            headers=headers,
            expiries=expiries,
            files=completed_status.files,
            result_format=transform.result_format,
            log_url=completed_status.log_url,
        )

    def cache_transform(self, record: TransformedResults):
        transforms = Query()
        with self.lock:
            self.db.upsert(
                json.loads(record.model_dump_json()), transforms.hash == record.hash
            )

    def update_record(self, record: TransformedResults):
        transforms = Query()
        with self.lock:
            self.db.update(
                json.loads(record.model_dump_json()), transforms.hash == record.hash
            )

    def contains_hash(self, hash: str) -> bool:
        """
        Check if the cache has completed records for a hash
        """
        transforms = Query()
        with self.lock:
            records = self.db.search(
                (transforms.hash == hash) & ~(transforms.status == "SUBMITTED")
            )
        return len(records) > 0

    def is_transform_request_submitted(self, hash_value: str) -> bool:
        """
        Returns True if request is submitted
        Returns False if the request is not in the cache at all
        or not submitted
        """
        transform = Query()
        with self.lock:
            records = self.db.search((transform.hash == hash_value))

        if not records:
            return False

        if "status" in records[0] and records[0]["status"] == "SUBMITTED":
            return True
        return False

    def get_transform_request_id(self, hash_value: str) -> Optional[str]:
        """
        Return the request id of cached record
        """
        transform = Query()

        with self.lock:
            records = self.db.search(transform.hash == hash_value)

        if not records or "request_id" not in records[0]:
            raise CacheException("Request Id not found")
        return records[0]["request_id"]

    def update_transform_status(self, hash_value: str, status: str) -> None:
        """
        Update the cached record status
        """
        transform = Query()
        with self.lock:
            self.db.upsert(
                {"hash": hash_value, "status": status}, transform.hash == hash_value
            )

    def update_transform_request_id(self, hash_value: str, request_id: str) -> None:
        """
        Update the cached record request id
        """
        transform = Query()
        with self.lock:
            self.db.upsert(
                {"hash": hash_value, "request_id": request_id},
                transform.hash == hash_value,
            )

    def cache_submitted_transform(
        self, transform: TransformRequest, request_id: str
    ) -> None:
        """Cache a transform that has been submitted but not completed."""

        record = {
            "hash": transform.compute_hash(),
            "title": transform.title,
            "codegen": transform.codegen,
            # Store the plain value so the memoized data matches what is on disk.
            "result_format": transform.result_format.value,
            "request_id": request_id,
            "status": "SUBMITTED",
            "submit_time": datetime.now(timezone.utc).isoformat(),
        }
        transforms = Query()
        with self.lock:
            self.db.upsert(record, transforms.hash == record["hash"])

    def get_transform_by_hash(self, hash: str) -> Optional[TransformedResults]:
        """
        Returns completed transformations by hash
        """
        transforms = Query()
        with self.lock:
            records = records = self.db.search(
                (transforms.hash == hash) & ~(transforms.status == "SUBMITTED")
            )

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for hash")
        else:
            return TransformedResults(**self._patch_missing_fields(records[0]))

    def get_transform_by_request_id(
        self, request_id: str
    ) -> Optional[TransformedResults]:
        """
        Returns completed transformed results using a request id
        """
        transforms = Query()

        with self.lock:
            records = self.db.search(transforms.request_id == request_id)

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for request_id")
        else:
            return TransformedResults(**self._patch_missing_fields(records[0]))

    def cache_path_for_transform(self, transform_status: TransformStatus) -> Path:
        assert self.config.cache_path is not None, "Cache path not set"
        base = Path(self.config.cache_path)
        result = Path(os.path.join(base, transform_status.request_id))
        result.mkdir(parents=True, exist_ok=True)
        return result

    def cached_queries(self) -> List[TransformedResults]:
        transforms = Query()

        with self.lock:
            result = [
                TransformedResults(**self._patch_missing_fields(doc))
                for doc in self.db.search(
                    transforms.request_id.exists() & ~(transforms.status == "SUBMITTED")
                )
            ]
        return result

    def queries_in_state(self, state: str) -> List[dict]:
        """Return all transform records in a given state."""
        transforms = Query()
        with self.lock:
            return [
                doc
                for doc in self.db.search(
                    (transforms.status == "SUBMITTED") & transforms.request_id.exists()
                )
            ]

    def delete_record_by_request_id(self, request_id: str):
        with self.lock:
            self.db.remove(where("request_id") == request_id)

    def delete_record_by_hash(self, hash: str):
        transforms = Query()
        with self.lock:
            self.db.remove(transforms.hash == hash)

    def _patch_missing_fields(self, rec: dict) -> dict:
        rv = rec.copy()
        if "headers" not in rv:
            rv["headers"] = [{}] * len(rv["file_list"])
        if "expiries" not in rv:
            rv["expiries"] = [sys.maxsize] * len(rv["file_list"])
        return rv
