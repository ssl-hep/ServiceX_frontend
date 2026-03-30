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
    def versions(self) -> list[str]:
        distinct_versions = {doc["version"] for doc in self.db.all()}
        return list(distinct_versions)

    def get_version(self, version: str) -> "Version":
        """
        Return all completed runs sharing the given version tag, sorted by submission time.
        """
        transforms = Query()
        return Version(
            version,
            self._sorted_results(
                self.db.search(
                    (transforms.version == version) & (transforms.status == "COMPLETE")
                )
            ),
        )

    def _sorted_results(self, records: list[dict]) -> list[TransformedResults]:
        records.sort(
            key=lambda x: datetime.fromisoformat(
                x["submit_time"].replace("Z", "+00:00")
            )
        )
        return [TransformedResults(**rec) for rec in records]

    def __getitem__(self, item: str) -> "Version":
        """Index the catalog by version tag: cat['v1.0']"""
        return self.get_version(item)


class Version:
    def __init__(self, version: str, results: list[TransformedResults]):
        self.version = version

        # this is where the latest feature now gets enforced
        # only the most recent sample gets added to the catalog if the version/sample pair
        # is not unique
        latest_by_title: dict[str, TransformedResults] = {}
        for r in results:
            latest_by_title[r.title] = r
        self.results = list(latest_by_title.values())

    @property
    def samples(self) -> list[str]:
        return list({r.title for r in self.results})

    def get_sample(self, title: str) -> TransformedResults:
        """Return the latest run for the given sample title within this version."""
        runs = [r for r in self.results if r.title == title]
        if not runs:
            raise KeyError(f"Sample {title!r} not found in version {self.version!r}")
        return runs[-1]

    def __getitem__(self, title: str) -> TransformedResults:
        """Index by sample title: cat['v1.0']['my_sample']"""
        return self.get_sample(title)

    def run_ids(self) -> list[str]:
        return [run.short_hash for run in self.results]

    def get_run(self, sha: str) -> TransformedResults:
        return next(filter(lambda x: x.short_hash == sha, self.results))

    # Note: removed the "latest" feature since it could get confusing when mixed w/ versioning

    def __repr__(self) -> str:
        return f"Version({self.version!r}, {len(self.results)} run(s))"
