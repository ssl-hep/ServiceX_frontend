from pathlib import Path
from rich.console import Console
from rich.table import Table


from servicex.catalog import Catalog

cat = Catalog(Path("/Users/bengal1/dev/IRIS-HEP/ServiceX_Client/cache-dir"))
table = Table(title="Catalog")
table.add_column("Sample Name", style="cyan")
for sample in cat.samples:
    table.add_row(sample)
console = Console()
console.print(table)

uproot_cat = cat["UprootRaw_YAML"]
table = Table(title="UprootRaw_YAML", width=25)
table.add_column("Version", style="cyan")
for v in uproot_cat.versions:
    table.add_row(v)
console = Console()
console.print(table)


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


table = Table(title="Latest Run Timestamp")
table.add_column("Submit Time", style="magenta")
table.add_row(str(uproot_cat.latest.submit_time))
console.print(table)

table = Table(title="Files for Version 1.0")
table.add_column("File Path", style="cyan")
for file_path in uproot_cat.get_version("1.0").file_list:
    table.add_row(str(file_path))
console.print(table)
