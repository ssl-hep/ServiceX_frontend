from pathlib import Path

from servicex.catalog import Catalog

cat = Catalog(Path("/Users/bengal1/dev/IRIS-HEP/ServiceX_Client/cache-dir"))
print(cat.samples)

uproot_cat = cat["UprootRaw_YAML"]
print(uproot_cat.get_versions())


from rich.console import Console
from rich.table import Table

table = Table(title="Uproot Catalog Runs")
table.add_column("SHA", style="cyan")
table.add_column("RequestID", style="yellow")
table.add_column("Submit Time", style="magenta")
table.add_column("Version", style="green")

for sha in uproot_cat.get_runs():
    run = uproot_cat.get_run(sha)
    table.add_row(sha, run.request_id, str(run.submit_time), run.version)

console = Console()
console.print(table)


print(uproot_cat.latest.submit_time)
print(uproot_cat.get_version("1.0").file_list)
