from datetime import datetime
from pathlib import Path
import os
import shutil

from tinydb import TinyDB, Query

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



def build_symlink_forest(catalog: Catalog, output_dir: Path) -> None:
    """
    Build a symlink forest from *catalog* under *output_dir*, organized by version.
    The forest gets re-made completely each call, so it should always perfectly match the catalog.

    For every version creates:
        <output_dir>/<version>/<sample>  ->  <absolute cache directory>

    Also writes an ``ff_helper.txt`` file to help with fastframes integration. Can be commented out if this should not be part core SX funmctionality.
    """
    versions = catalog.versions

    if not versions:
        print("No versions found in catalog.")
        return

    if output_dir.exists():
        # Note: this wipes the forest and starts over again. But should be careful if this gets pointed to a place other than the "symlink" directory in the cache
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    for version_tag in sorted(versions):
        version = catalog.get_version(version_tag)
        version_dir = output_dir / version_tag
        version_dir.mkdir(parents=True, exist_ok=True)

        for sample_title in sorted(version.samples):
            result = version.get_sample(sample_title)
            if not result.file_list:
                print(f"  [{version_tag}] {sample_title}: No files found in catalog, skipping symlink.")
                continue

            cached_path = Path(result.file_list[0]).parent
            sample_symlink_path = version_dir / sample_title
            os.symlink(cached_path, sample_symlink_path)

            print(f"  [{version_tag}] {sample_title}: symlink -> {cached_path}")

        ff_helper_file = version_dir / "ff_helper.txt"
        version_abs_path = version_dir.resolve()
        with open(ff_helper_file, "w") as f:
            f.write(
                "Hello intrepid serviceX user! Please run this command in the appropriate "
                "directory to generate input metadata files for fastframe consumption:\n\n"
            )
            f.write(
                f"python3 fastframes/python/produce_metadata_files.py --root_files_folder {version_abs_path}\n"
            )
