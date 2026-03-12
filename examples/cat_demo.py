from pathlib import Path
from rich.console import Console
from rich.table import Table


from servicex.catalog import Catalog

cat = Catalog(Path("/data/tost/sx_versioning_cache"))
console = Console()

table = Table(title="Catalog Versions")
table.add_column("Version", style="cyan")
for version in cat.versions:
    table.add_row(version)
console.print(table)

v1 = cat["1.0"]

table = Table(title="Samples in Version 1.0")
table.add_column("Sample Name", style="cyan")
for sample in v1.samples:
    table.add_row(sample)
console.print(table)

table = Table(title="Runs in Version 1.0")
table.add_column("SHA", style="cyan")
table.add_column("RequestID", style="yellow")
table.add_column("Submit Time", style="magenta")
table.add_column("Version", style="green")
table.add_column("Sample Name", style="blue")
for sha in v1.run_ids():
    run = v1.get_run(sha)
    table.add_row(sha, run.request_id, str(run.submit_time), run.version, run.title)
console.print(table)

