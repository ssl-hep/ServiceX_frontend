from pathlib import Path

from servicex.catalog import Catalog

cat = Catalog(Path("/Users/bengal1/dev/IRIS-HEP/ServiceX_Client/cache-dir"))
print(cat.samples)

print(cat.db.all())
uproot_cat = cat["UprootRaw_YAML"]
print(uproot_cat.get_runs())

for sha in uproot_cat.get_runs():
    print(sha, uproot_cat.get_run(sha).submit_time)


print(uproot_cat.latest.submit_time)
print(uproot_cat.latest.file_list)