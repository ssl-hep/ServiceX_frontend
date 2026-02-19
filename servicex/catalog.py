from datetime import datetime
from pathlib import Path

from tinydb import TinyDB, Query
import os

from servicex.models import TransformedResults


class Catalog:
    def __init__(self, path: Path):
        self.path = path
        self.db = TinyDB(os.path.join(self.path, ".servicex", "db.json"))

    @property
    def samples(self) -> list[str]:
        distinct_titles = {doc["title"] for doc in self.db.all()}
        return list(distinct_titles)

    def get_sample(self, title: str) -> list[TransformedResults]:
        """
        Get a sample from the database and sort the runs by submission time. Decode
        the JSON into TransformedResults objects.
        Return a list of TransformedResults, sorted by submission time
        """
        transforms = Query()

        sample_recs = self.db.search(
            (transforms.title == title) & (transforms.status == "COMPLETE")
        )
        sample_recs.sort(
            key=lambda x: datetime.fromisoformat(
                x["submit_time"].replace("Z", "+00:00")
            )
        )

        return [TransformedResults(**rec) for rec in sample_recs]

    def __getitem__(self, item):
        """
        Allow the catalog to be indexed by sample title."""
        return Sample(self.get_sample(item))


class Sample:
    def __init__(self, results: list[TransformedResults]):
        self.results = results

    def get_runs(self) -> list[str]:
        return [run.short_hash for run in self.results]

    @property
    def versions(self) -> list[str]:
        return [run.version for run in self.results if run.version is not None]

    def get_run(self, sha: str) -> TransformedResults:
        return next(filter(lambda x: x.short_hash == sha, self.results))

    def get_version(self, version: str) -> TransformedResults:
        return next(filter(lambda x: x.version == version, self.results))

    @property
    def latest(self) -> TransformedResults:
        return self.results[-1]
