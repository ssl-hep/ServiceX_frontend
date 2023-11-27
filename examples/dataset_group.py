from servicex import ServiceXClient, ResultFormat, DatasetGroup
from rich.console import Console
from rich.table import Table

dataset_id = "rucio://user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v8"

sx = ServiceXClient(backend="uproot")
ds_raw = sx.func_adl_dataset(
    dataset_id,
    codegen="uproot",
    title="Dataset Group Example",
    num_files=3,
).Select(lambda e: {'el_pt': e['el_pt']})

trees = ["nominal", "EG_RESOLUTION_ALL__1down",
         "EG_RESOLUTION_ALL__1up",
         "MUON_SAGITTA_RESBIAS__1down",
         "MUON_SAGITTA_RESBIAS__1up"]

group = DatasetGroup([
    ds_raw.set_tree(branch).set_title(branch) for branch in trees])

group.set_result_format(ResultFormat.parquet)

results = group.as_signed_urls()

table = Table(title="Dataset Group Example")

table.add_column("Tree", justify="right", style="cyan", no_wrap=True)
table.add_column("URLs", style="magenta")
for result in results:
    table.add_row(result.title, str(result.signed_url_list))

console = Console()
console.print(table)
