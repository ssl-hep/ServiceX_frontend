# Copyright (c) 2026, IRIS-HEP
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
"""
Build Salt-ready HDF5 training files with ServiceX.

The transformers run :func:`run_query`, configured by a user config, on every input
file: branch selection, event cuts and conversion to the Salt layout (one structured
array per object, jagged objects padded with a ``valid`` mask). Each input file gives
one ``.h5`` file, padded to the largest multiplicity found in that file.

On the client, :func:`merge_salt_files` concatenates the per-file outputs into a single
Salt file, bringing every jagged object to a common width.

Configuration (YAML or dict)::

    tree_name: reco
    cuts:                       # UprootRaw-style expressions, combined with "&"
      - "num(jet_pt_NOSYS[jet_pt_NOSYS > 25000]) > 1"
    objects:                    # same layout as ml_framework's to_hdf5 config
      event:
        branches: {met: met_met_NOSYS}
        attach_label: true      # the per-sample label is added to this object
      jet:
        branches: {pt: jet_pt_NOSYS, eta: jet_eta}
    step_size: 200000           # optional, events read per chunk on the transformers
    output_file: salt_data.h5   # optional (client side)
    max_events: 10000           # optional, per sample (client side)

Samples (YAML or dict)::

    Hplus_cb_20GeV_mc20a:
      dataset: user.alheld:user.alheld.561444...   # Rucio DID
      label: 1
    ttbar:
      files: [root://..., root://...]              # or an explicit list of files
      label: 0
"""

import inspect
import logging
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

import yaml

from servicex.dataset_identifier import FileListDataset, RucioDatasetIdentifier
from servicex.python_dataset import PythonFunction
from servicex.servicex_client import ProgressBarFormat, deliver

logger = logging.getLogger(__name__)

# Config keys that are sent to the transformers; the others are only used on the client
_TRANSFORMER_KEYS = ("tree_name", "cuts", "objects", "step_size")


# ---------------------------------------------------------------------------
# Transformer side
# ---------------------------------------------------------------------------


def run_query(input_filenames, output_path):
    """
    Runs on the ServiceX transformers (two-argument PythonFunction form: the function
    writes the output file itself).

    ``SALT_CONFIG`` and ``SALT_LABEL`` are module-level constants of the code sent to the
    transformers: :func:`build_salt_query` writes them in front of this function's source.
    Everything else must be imported inside the function, and the source must stay ASCII.
    """
    import re

    import awkward as ak
    import h5py
    import numpy as np
    import uproot

    config = SALT_CONFIG  # noqa: F821
    label = SALT_LABEL  # noqa: F821
    objects = config["objects"]
    cut_expr = " & ".join("(" + c + ")" for c in config.get("cuts") or [])

    # UprootRaw-style functions for the cut expressions (reductions act per event)
    def _per_event(func):
        return lambda x: func(x, axis=-1)

    cut_functions = {"num": lambda x: ak.num(x, axis=1), "abs": abs}
    for name in (
        "count",
        "count_nonzero",
        "any",
        "all",
        "sum",
        "prod",
        "min",
        "max",
        "argmin",
        "argmax",
        "mean",
        "var",
        "std",
    ):
        cut_functions[name] = _per_event(getattr(ak, name))

    def to_tables(arr):
        """Salt layout: one structured numpy array per object."""
        tables = {}
        for obj, obj_cfg in objects.items():
            branch_map = obj_cfg["branches"]
            first = arr[next(iter(branch_map.values()))]
            jagged = first.ndim > 1
            columns = {}
            valid = None
            width = 0
            if jagged:
                # automatic padding: largest multiplicity in this chunk
                counts = ak.num(first, axis=1)
                width = int(ak.max(counts)) if len(first) else 0
            for name, branch in branch_map.items():
                values = arr[branch]
                if jagged:
                    padded = ak.pad_none(values, width, axis=1, clip=True)
                    if valid is None:
                        valid = ak.to_numpy(~ak.is_none(padded, axis=-1))
                    values = ak.fill_none(padded, 0)
                columns[name] = ak.to_numpy(values)
            if obj_cfg.get("attach_label", False):
                columns["label"] = np.full(len(arr), label, dtype=np.int64)
            fields = [(name, col.dtype) for name, col in columns.items()]
            if jagged:
                # required by Salt for variable-length objects
                fields.append(("valid", "?"))
            table = np.zeros((len(arr), width) if jagged else (len(arr),), dtype=fields)
            for name, col in columns.items():
                table[name] = col
            if jagged and len(arr) and width:
                table["valid"] = valid
            tables[obj] = (table, jagged)
        return tables

    def append(out, obj, table, jagged):
        """Append a chunk; jagged datasets grow in width when a chunk needs more slots."""
        if obj not in out:
            if jagged:
                out.create_dataset(
                    obj,
                    shape=(0, 0),
                    maxshape=(None, None),
                    dtype=table.dtype,
                    chunks=(1024, 8),
                    compression="gzip",
                )
            else:
                out.create_dataset(
                    obj,
                    shape=(0,),
                    maxshape=(None,),
                    dtype=table.dtype,
                    chunks=(4096,),
                    compression="gzip",
                )
        ds = out[obj]
        table = table.astype(ds.dtype, copy=False)
        n_old, n_new = ds.shape[0], table.shape[0]
        if jagged:
            width = max(ds.shape[1], table.shape[1])
            if width > ds.shape[1]:
                ds.resize(width, axis=1)  # new slots are zero: valid=False
            if table.shape[1] < width:
                padded = np.zeros((n_new, width), dtype=ds.dtype)
                padded[:, : table.shape[1]] = table
                table = padded
        if n_new:
            ds.resize(n_old + n_new, axis=0)
            if not jagged or ds.shape[1]:
                ds[n_old:] = table

    n_events_in = 0
    with uproot.open(input_filenames) as f, h5py.File(output_path, "w") as out:
        tree = f[config["tree_name"]]
        available = set(tree.keys())

        # 1) column selection: object branches + branches used in the cuts
        object_branches = [b for o in objects.values() for b in o["branches"].values()]
        missing = sorted(set(object_branches) - available)
        if missing:
            raise ValueError("Branches not found in tree: " + ", ".join(missing))
        cut_branches = [
            n for n in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cut_expr) if n in available
        ]
        branches = list(dict.fromkeys(object_branches + cut_branches))

        # step size in entries: a memory step size ("100 MB") makes uproot read basket
        # sizes, which fails through fsspec-xrootd on the transformers
        chunks = tree.iterate(
            branches, step_size=config.get("step_size", 200000), library="ak"
        )
        for arr in chunks:
            n_events_in += len(arr)
            # 2) event cuts
            if cut_expr:
                namespace = dict(cut_functions)
                namespace.update({b: arr[b] for b in arr.fields})
                mask = eval(cut_expr, {"__builtins__": {}}, namespace)
                arr = arr[mask]
            # 3) Salt layout, appended to the output file
            for obj, (table, jagged) in to_tables(arr).items():
                append(out, obj, table, jagged)

        if len(out.keys()) < len(objects):  # empty tree: still write every dataset
            for obj, (table, jagged) in to_tables(
                tree.arrays(branches, entry_stop=0)
            ).items():
                append(out, obj, table, jagged)

        out.attrs["input_file"] = str(input_filenames)
        out.attrs["label"] = label
        out.attrs["n_events_in"] = n_events_in
        out.attrs["n_events"] = out[next(iter(objects))].shape[0]


# ---------------------------------------------------------------------------
# Configuration and request
# ---------------------------------------------------------------------------


def _read_yaml(source: Union[str, Path, Mapping]) -> Dict[str, Any]:
    if isinstance(source, Mapping):
        return dict(source)
    with open(source, "r") as f:
        content = yaml.safe_load(f)
    if not isinstance(content, dict):
        raise ValueError(f"{source}: expected a YAML mapping at the top level")
    return content


def load_salt_config(config: Union[str, Path, Mapping]) -> Dict[str, Any]:
    """Read and validate the Salt config (path to a YAML file, or a dict)."""
    config = _read_yaml(config)
    if not config.get("tree_name"):
        raise ValueError("Salt config: 'tree_name' is required")
    objects = config.get("objects")
    if not isinstance(objects, Mapping) or not objects:
        raise ValueError("Salt config: 'objects' must be a non-empty mapping")
    for obj, obj_cfg in objects.items():
        branches = obj_cfg.get("branches") if isinstance(obj_cfg, Mapping) else None
        if not isinstance(branches, Mapping) or not branches:
            raise ValueError(
                f"Salt config: objects.{obj}.branches must be a non-empty mapping"
            )
    if not any(o.get("attach_label", False) for o in objects.values()):
        raise ValueError("Salt config: one object needs 'attach_label: true'")
    cuts = config.get("cuts") or []
    if isinstance(cuts, str):
        cuts = [cuts]
    config["cuts"] = list(cuts)
    return config


def load_salt_samples(samples: Union[str, Path, Mapping]) -> Dict[str, Dict[str, Any]]:
    """
    Read and validate the samples (path to a YAML file, or a dict).
    Each sample needs a ``label`` and either ``dataset`` (Rucio DID) or ``files``.
    """
    samples = _read_yaml(samples)
    if set(samples) == {"samples"} and isinstance(samples["samples"], Mapping):
        samples = dict(samples["samples"])
    if not samples:
        raise ValueError("Salt samples: no samples given")
    for name, sample in samples.items():
        if not isinstance(sample, Mapping):
            raise ValueError(f"Salt samples: '{name}' must be a mapping")
        if "label" not in sample or isinstance(sample["label"], bool):
            raise ValueError(f"Salt samples: '{name}' needs an integer 'label'")
        int(sample["label"])
        if ("dataset" in sample) == ("files" in sample):
            raise ValueError(
                f"Salt samples: '{name}' needs either 'dataset' or 'files'"
            )
    return samples


def build_salt_query(config: Mapping, label: int) -> PythonFunction:
    """PythonFunction query: :func:`run_query` with the config and label in front of it."""
    transformer_config = {k: config[k] for k in _TRANSFORMER_KEYS if k in config}
    source = (
        f"SALT_CONFIG = {transformer_config!r}\n"
        f"SALT_LABEL = {int(label)!r}\n\n\n" + inspect.getsource(run_query)
    )
    try:
        source.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("Salt config must only contain ASCII characters")
    compile(source, "salt_run_query", "exec")
    return PythonFunction().with_uproot_function(source)


def _dataset_for(sample: Mapping):
    if "files" in sample:
        files = sample["files"]
        return FileListDataset([files] if isinstance(files, str) else list(files))
    return RucioDatasetIdentifier(sample["dataset"], num_files=sample.get("nfiles"))


def build_salt_spec(
    config: Mapping, samples: Mapping, delivery: str = "LocalCache"
) -> Dict[str, Any]:
    """ServiceX spec: one sample per dataset, one query per label."""
    queries: Dict[int, PythonFunction] = {}
    sample_list = []
    for name, sample in samples.items():
        label = int(sample["label"])
        if label not in queries:
            queries[label] = build_salt_query(config, label)
        entry = {"Name": name, "Dataset": _dataset_for(sample), "Query": queries[label]}
        if "files" in sample and sample.get("nfiles"):
            entry["NFiles"] = int(sample["nfiles"])
        sample_list.append(entry)
    return {
        # OutputFormat only sets the extension: the files written by run_query are HDF5
        "General": {"Delivery": delivery, "OutputFormat": "parquet"},
        "Sample": sample_list,
    }


def deliver_salt_files(
    config: Union[str, Path, Mapping],
    samples: Union[str, Path, Mapping],
    delivery: str = "LocalCache",
    **deliver_kwargs,
) -> Dict[str, List[str]]:
    """
    Run the transformation and return ``{sample: [files or URLs]}``.
    Samples whose transform failed are logged and left out.
    """
    spec = build_salt_spec(
        load_salt_config(config), load_salt_samples(samples), delivery
    )
    results = deliver(spec, **deliver_kwargs)
    files: Dict[str, List[str]] = {}
    for name, result in results.items():
        try:
            paths = [str(f) for f in result]
        except Exception as err:
            logger.error(f"Salt data: no results for sample '{name}': {err}")
            continue
        if not paths:  # e.g. a transform that completed with failed files
            logger.error(f"Salt data: no output files for sample '{name}'")
            continue
        files[name] = paths
    return files


# ---------------------------------------------------------------------------
# Client side: merge
# ---------------------------------------------------------------------------


def _file_key(path: str) -> str:
    """Sort key: output file names contain the input file name."""
    return os.path.basename(str(path).split("?")[0])


def _download(url: str, directory: str, retries: int = 5) -> str:
    import httpx

    # own sub-directory: two samples may share an input file name
    target = os.path.join(tempfile.mkdtemp(dir=directory), _file_key(url))
    for attempt in range(retries):
        try:
            with httpx.stream(
                "GET", url, follow_redirects=True, timeout=120
            ) as response:
                response.raise_for_status()
                with open(target, "wb") as f:
                    for block in response.iter_bytes(1 << 20):
                        f.write(block)
            return target
        except (httpx.TransportError, httpx.HTTPStatusError) as err:
            retryable = not isinstance(err, httpx.HTTPStatusError) or (
                err.response.status_code >= 500
            )
            if not retryable or attempt == retries - 1:
                raise
            logger.warning(f"Salt merge: download failed ({err}), retrying")
            time.sleep(2**attempt)
    return target


def _plan_sample(paths, max_events, tmp_dir):
    """Files of one sample (in input-file order) and how many events to take from each."""
    import h5py

    plan = []
    n_left = max_events if max_events and max_events > 0 else None
    for path in sorted(paths, key=_file_key):
        if n_left is not None and n_left <= 0:
            break
        local = (
            _download(path, tmp_dir)
            if str(path).startswith(("http://", "https://"))
            else str(path)
        )
        with h5py.File(local, "r") as f:
            layout = {
                obj: (f[obj].dtype, f[obj].shape[1] if f[obj].ndim > 1 else None)
                for obj in f.keys()
            }
            n_file = f[next(iter(f.keys()))].shape[0]
        n_take = n_file if n_left is None else min(n_file, n_left)
        plan.append((local, n_take, layout))
        if n_left is not None:
            n_left -= n_take
    return plan


def _merged_dtype(dtypes):
    names = dtypes[0].names
    for dt in dtypes[1:]:
        if dt.names != names:
            raise ValueError(f"Salt merge: inconsistent fields {names} vs {dt.names}")
    import numpy as np

    return np.dtype([(n, np.result_type(*[dt[n] for dt in dtypes])) for n in names])


def merge_salt_files(
    files: Mapping[str, List[str]],
    output_file: Union[str, Path],
    max_events: Optional[int] = None,
    compression: Optional[str] = None,
    max_workers: int = 8,
) -> Dict[str, Any]:
    """
    Concatenate the per-file outputs of :func:`run_query` into one Salt file.

    Each jagged object is brought to a common width: the largest multiplicity among the
    selected events (files are padded with ``valid=False`` slots). ``max_events`` keeps the
    first N events of each sample. URLs (``Delivery: URLs``) are downloaded on the fly,
    only as far as needed for ``max_events``.

    Returns a summary: output file, events per sample, widths and timing.
    """
    import h5py
    import numpy as np

    start = time.perf_counter()
    tmp_dir = tempfile.mkdtemp(prefix="servicex_salt_")
    try:
        # pass 1: which files / events, dtypes and widths (samples in parallel)
        names = list(files)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            plans = dict(
                zip(
                    names,
                    pool.map(
                        lambda n: _plan_sample(files[n], max_events, tmp_dir), names
                    ),
                )
            )
        entries = [(name, *p) for name in names for p in plans[name] if p[1] > 0]
        if not entries:
            raise ValueError("Salt merge: no events to merge")

        objects = list(entries[0][3])
        dtypes, widths = {}, {}
        for obj in objects:
            dtypes[obj] = _merged_dtype([e[3][obj][0] for e in entries])
            ws = [e[3][obj][1] for e in entries]
            widths[obj] = None if ws[0] is None else max(ws)
        n_total = sum(e[2] for e in entries)

        # pass 2: copy each selected block at its offset, padded to the common width
        used = {obj: 0 for obj in objects}
        with h5py.File(output_file, "w") as out:
            for obj in objects:
                w = widths[obj]
                shape = (n_total,) if w is None else (n_total, w)
                out.create_dataset(
                    obj,
                    shape=shape,
                    dtype=dtypes[obj],
                    maxshape=(n_total,) if w is None else (n_total, None),
                    chunks=True if n_total else None,
                    compression=compression,
                )
            start_row = 0
            for _, local, n_take, _ in entries:
                rows = slice(start_row, start_row + n_take)
                with h5py.File(local, "r") as f:
                    for obj in objects:
                        block = f[obj][:n_take]
                        w = widths[obj]
                        if w is None:
                            out[obj][rows] = block.astype(dtypes[obj])
                            continue
                        fitted = np.zeros((n_take, w), dtype=dtypes[obj])
                        k = min(block.shape[1], w)
                        if k:
                            for field in dtypes[obj].names:
                                fitted[field][:, :k] = block[field][:, :k]
                            filled = np.flatnonzero(block["valid"][:, :k].any(axis=0))
                            if filled.size:
                                used[obj] = max(used[obj], int(filled[-1]) + 1)
                            out[obj][rows] = fitted
                start_row += n_take

            # trim slots that no selected event uses (files padded wider than needed)
            for obj in objects:
                if widths[obj] is not None and used[obj] < widths[obj]:
                    out[obj].resize(used[obj], axis=1)
                    widths[obj] = used[obj]

        events = {}
        for name, _, n_take, _ in entries:
            events[name] = events.get(name, 0) + n_take
        return {
            "output_file": str(output_file),
            "n_events": n_total,
            "events_per_sample": events,
            "widths": {o: w for o, w in widths.items() if w is not None},
            "merge_seconds": time.perf_counter() - start,
        }
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# All in one
# ---------------------------------------------------------------------------


def get_salt_data(
    config: Union[str, Path, Mapping],
    samples: Union[str, Path, Mapping],
    output_file: Optional[Union[str, Path]] = None,
    max_events: Optional[int] = None,
    delivery: str = "LocalCache",
    servicex_name: Optional[str] = None,
    config_path: Optional[str] = None,
    ignore_local_cache: bool = False,
    progress_bar: ProgressBarFormat = ProgressBarFormat.default,
) -> Dict[str, Any]:
    """
    Build a Salt training file: transform every sample on ServiceX, then merge.

    :param config: Salt config (YAML path or dict): tree, cuts, objects.
    :param samples: samples (YAML path or dict): ``{name: {dataset|files, label}}``.
    :param output_file: merged file; defaults to ``output_file`` in the config,
        else ``salt_data.h5``.
    :param max_events: first N events per sample; defaults to ``max_events`` in the config.
    :param delivery: ``LocalCache`` (download every output) or ``URLs`` (download only
        what the merge needs).
    :return: merge summary, plus ServiceX timing and failed samples.
    """
    config = load_salt_config(config)
    samples = load_salt_samples(samples)
    if output_file is None:
        output_file = config.get("output_file", "salt_data.h5")
    if max_events is None:
        max_events = config.get("max_events")

    start = time.perf_counter()
    files = deliver_salt_files(
        config,
        samples,
        delivery=delivery,
        servicex_name=servicex_name,
        config_path=config_path,
        ignore_local_cache=ignore_local_cache,
        progress_bar=progress_bar,
    )
    servicex_seconds = time.perf_counter() - start
    if not files:
        raise RuntimeError("Salt data: no sample was delivered")

    summary = merge_salt_files(files, output_file, max_events=max_events)
    summary["servicex_seconds"] = servicex_seconds
    summary["failed_samples"] = [s for s in samples if s not in files]
    return summary
