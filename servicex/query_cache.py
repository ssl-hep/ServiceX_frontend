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
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timezone
from filelock import FileLock
from tinydb import TinyDB, Query, where
import logging

from servicex.configuration import Configuration
from servicex.models import TransformRequest, TransformStatus, TransformedResults


class CacheException(Exception):
    pass


logger = logging.getLogger(__name__)


class QueryCache:
    def __init__(self, config: Configuration):
        self.config = config
        self._mem_cache: Optional[List[dict]] = None
        if self.config.cache_path is not None:
            Path(self.config.cache_path).mkdir(parents=True, exist_ok=True)
            Path(self.config.cache_path + "/.servicex").mkdir(
                parents=True, exist_ok=True
            )
            self.db = TinyDB(
                os.path.join(self.config.cache_path, ".servicex", "db.json")
            )
            self.lock = FileLock(
                os.path.join(self.config.cache_path, ".servicex", "db.lock")
            )

    def close(self):
        self.db.close()

    def _load_all(self) -> List[dict]:
        """Return all DB records, using an in-memory cache to avoid repeated JSON parses."""
        if self._mem_cache is None:
            with self.lock:
                self._mem_cache = self.db.all()
        return self._mem_cache

    def _invalidate(self) -> None:
        """Invalidate the in-memory cache after a write."""
        self._mem_cache = None

    def transformed_results(
        self,
        transform: TransformRequest,
        completed_status: TransformStatus,
        data_dir: str,
        file_list: List[str],
        signed_urls,
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
        self._invalidate()

    def update_record(self, record: TransformedResults):
        transforms = Query()
        with self.lock:
            self.db.update(
                json.loads(record.model_dump_json()), transforms.hash == record.hash
            )
        self._invalidate()

    def contains_hash(self, hash: str) -> bool:
        """
        Check if the cache has completed records for a hash
        """
        return any(
            doc.get("hash") == hash and doc.get("status") != "SUBMITTED"
            for doc in self._load_all()
        )

    def is_transform_request_submitted(self, hash_value: str) -> bool:
        """
        Returns True if request is submitted
        Returns False if the request is not in the cache at all
        or not submitted
        """
        records = [doc for doc in self._load_all() if doc.get("hash") == hash_value]
        if not records:
            return False
        return records[0].get("status") == "SUBMITTED"

    def get_transform_request_id(self, hash_value: str) -> Optional[str]:
        """
        Return the request id of cached record
        """
        records = [doc for doc in self._load_all() if doc.get("hash") == hash_value]
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
        self._invalidate()

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
        self._invalidate()

    def cache_submitted_transform(
        self, transform: TransformRequest, request_id: str
    ) -> None:
        """Cache a transform that has been submitted but not completed."""

        record = {
            "hash": transform.compute_hash(),
            "title": transform.title,
            "codegen": transform.codegen,
            "result_format": transform.result_format,
            "request_id": request_id,
            "status": "SUBMITTED",
            "submit_time": datetime.now(timezone.utc).isoformat(),
        }
        transforms = Query()
        with self.lock:
            self.db.upsert(record, transforms.hash == record["hash"])
        self._invalidate()

    def get_transform_by_hash(self, hash: str) -> Optional[TransformedResults]:
        """
        Returns completed transformations by hash
        """
        records = [
            doc
            for doc in self._load_all()
            if doc.get("hash") == hash and doc.get("status") != "SUBMITTED"
        ]

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for hash")
        else:
            return TransformedResults(**records[0])

    def get_transform_by_request_id(
        self, request_id: str
    ) -> Optional[TransformedResults]:
        """
        Returns completed transformed results using a request id
        """
        records = [
            doc for doc in self._load_all() if doc.get("request_id") == request_id
        ]

        if not records:
            return None

        if len(records) != 1:
            raise CacheException("Multiple records found in db for request_id")
        else:
            return TransformedResults(**records[0])

    def cache_path_for_transform(self, transform_status: TransformStatus) -> Path:
        assert self.config.cache_path is not None, "Cache path not set"
        base = Path(self.config.cache_path)
        result = Path(os.path.join(base, transform_status.request_id))
        result.mkdir(parents=True, exist_ok=True)
        return result

    def cached_queries(self) -> List[TransformedResults]:
        return [
            TransformedResults(**doc)
            for doc in self._load_all()
            if "request_id" in doc and doc.get("status") != "SUBMITTED"
        ]

    def queries_in_state(self, state: str) -> List[dict]:
        """Return all transform records in a given state."""
        return [
            doc
            for doc in self._load_all()
            if doc.get("status") == state and "request_id" in doc
        ]

    def delete_record_by_request_id(self, request_id: str):
        with self.lock:
            self.db.remove(where("request_id") == request_id)
        self._invalidate()

    def delete_record_by_hash(self, hash: str):
        transforms = Query()
        with self.lock:
            self.db.remove(transforms.hash == hash)
        self._invalidate()
