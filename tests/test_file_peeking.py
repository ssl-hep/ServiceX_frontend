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
import re

import pytest
import uproot
import awkward as ak
import numpy as np
from packaging.version import Version

from servicex.tools import file_peeking
from servicex import dataset
from servicex.python_dataset import PythonFunction
from servicex.dataset_identifier import (
    RucioDatasetIdentifier,
    FileListDataset,
    CERNOpenDataDatasetIdentifier,
    XRootDDatasetIdentifier,
)

# Writing RNTuples with uproot is only supported from 5.7 onwards
requires_rntuple_writing = pytest.mark.skipif(
    Version(uproot.__version__) < Version("5.7"),
    reason="uproot < 5.7 can't write RNTuples",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def raw_root_file(tmp_path):
    """A plain ROOT file, as if produced by the experiment (input to run_query)."""
    test_path = str(tmp_path / "test_file1.root")
    tree_data1 = {"branch1": np.ones(100), "branch2": np.zeros(100)}
    tree_data2 = {"branch1": np.ones(10)}

    # Explicit mktree: newer uproot writes RNTuples by default with file[name] = ...
    with uproot.create(test_path) as file:
        file.mktree("background", {"branch1": np.float64, "branch2": np.float64})
        file["background"].extend(tree_data1)
        file.mktree("signal", {"branch1": np.float64})
        file["signal"].extend(tree_data2)

    return test_path


def _write_delivered(raw_path, out_path):
    """
    Mimics a ServiceX `deliver()` output file: run_query() is executed against
    the raw file, and its returned {tree_name: array} dict is written back out
    as top-level trees, as ServiceX does with OutputFormat root-rntuple.
    """
    trees = file_peeking.run_query(raw_path)
    with uproot.create(out_path) as f:
        for tree_name, arr in trees.items():
            # Write column by column: uproot can't write a record with a string field
            f[tree_name] = {field: arr[field] for field in arr.fields}
    return str(out_path)


@pytest.fixture
def deliver_like_file(raw_root_file, tmp_path):
    """Single-sample deliver() output dict."""
    out_path = _write_delivered(raw_root_file, tmp_path / "delivered.root")
    return {"test_sample": [out_path]}


@pytest.fixture
def deliver_like_multi(raw_root_file, tmp_path):
    """Two-sample deliver() output dict, each sample with different trees."""
    other_raw = str(tmp_path / "test_file2.root")
    with uproot.create(other_raw) as file:
        file.mktree("jets", {"jet_pt": np.float64})
        file["jets"].extend({"jet_pt": np.ones(5)})

    path_a = _write_delivered(raw_root_file, tmp_path / "delivered_a.root")
    path_b = _write_delivered(other_raw, tmp_path / "delivered_b.root")
    return {"sample_A": [path_a], "sample_B": [path_b]}


@pytest.fixture
def raw_rntuple_file(tmp_path):
    """A plain ROOT file holding an RNTuple, with a jagged and a flat column."""
    test_path = str(tmp_path / "test_rntuple.root")
    data = ak.Array(
        {
            "el_pt": [[10.0, 20.0], [30.0]],
            "n_el": np.array([2, 1], dtype=np.int32),
        }
    )
    with uproot.create(test_path) as file:
        file.mkrntuple("events", data)

    return test_path


# ---------------------------------------------------------------------------
# run_query
# ---------------------------------------------------------------------------
def test_run_query(raw_root_file):
    trees = file_peeking.run_query(raw_root_file)

    assert isinstance(trees, dict), "run_query() should return a dict of trees"
    assert set(trees.keys()) == {"background", "signal"}

    for name in ("background", "signal"):
        assert isinstance(trees[name], ak.Array)
        assert "_classname" in trees[name].fields
        assert trees[name]["_classname"][0] == "TTree"

    assert set(trees["background"].fields) == {"branch1", "branch2", "_classname"}
    assert trees["background"]["branch1"][0] == 1.0
    assert trees["background"]["branch2"][0] == 0.0

    assert set(trees["signal"].fields) == {"branch1", "_classname"}
    assert trees["signal"]["branch1"][0] == 1.0


@requires_rntuple_writing
def test_run_query_rntuple(raw_rntuple_file):
    trees = file_peeking.run_query(raw_rntuple_file)

    assert set(trees.keys()) == {"events"}
    events = trees["events"]
    assert isinstance(events, ak.Array)
    assert set(events.fields) == {"el_pt", "n_el", "_classname"}
    assert events["_classname"][0] == "ROOT::RNTuple"

    # Only the first entry is read, jagged structure is preserved
    assert len(events) == 1
    assert events["el_pt"][0].tolist() == [10.0, 20.0]
    assert events["n_el"][0] == 2


# ---------------------------------------------------------------------------
# _build_deliver_spec
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "datasets, expected_size, expected_names, expected_ds_types",
    [
        ("random_space:did", 1, ["random_space:did"], [RucioDatasetIdentifier]),
        (
            ["random_space:did", "random_space:did2"],
            2,
            ["random_space:did", "random_space:did2"],
            [RucioDatasetIdentifier, RucioDatasetIdentifier],
        ),
        (
            {"Custom-Name": "random_space:did"},
            1,
            ["Custom-Name"],
            [RucioDatasetIdentifier],
        ),
    ],
)
def test_build_deliver_spec(datasets, expected_size, expected_names, expected_ds_types):
    spec = file_peeking._build_deliver_spec(datasets)

    assert isinstance(spec, dict)
    assert "Sample" in spec and isinstance(spec["Sample"], list)
    assert spec.get("General") == {"OutputFormat": "root-rntuple"}
    assert len(spec["Sample"]) == expected_size

    for entry, name, ds_type in zip(spec["Sample"], expected_names, expected_ds_types):
        assert isinstance(entry, dict)
        assert entry["NFiles"] == 1
        assert entry["Name"] == name
        assert isinstance(entry["Dataset"], ds_type)
        assert isinstance(entry["Query"], PythonFunction)


def test_build_deliver_spec_with_dataset_identifiers():
    ds_list = [
        dataset.Rucio("random_space:did"),
        dataset.XRootD("root://server/file.root"),
        dataset.CERNOpenData("cernopendata:12345"),
        dataset.FileList(["file1.root", "file2.root"]),
    ]
    ds_types = [
        RucioDatasetIdentifier,
        XRootDDatasetIdentifier,
        CERNOpenDataDatasetIdentifier,
        FileListDataset,
    ]

    for did, did_type in zip(ds_list, ds_types):
        spec = file_peeking._build_deliver_spec(did)
        entry = spec["Sample"][0]
        assert len(spec["Sample"]) == 1
        assert isinstance(entry["Dataset"], did_type)
        assert isinstance(entry["Query"], PythonFunction)


def test_build_deliver_spec_wrong_type():
    wrong_did = 1234
    expected_msg = (
        f"Unsupported dataset input type: {type(wrong_did)}.\n"
        "Input must be str or list of str of Rucio DIDs, "
        "a DataSetIdentifier object or a dict ('sample_name':'dataset_id')"
    )
    with pytest.raises(ValueError, match=re.escape(expected_msg)):
        file_peeking._build_deliver_spec(wrong_did)


# ---------------------------------------------------------------------------
# _show_structure
# ---------------------------------------------------------------------------
def test_show_structure_content(deliver_like_file):
    output_str = file_peeking._show_structure(deliver_like_file)

    assert isinstance(output_str, str)
    assert "\U0001f4c1 Sample: test_sample" in output_str
    assert "File structure with column filter '':" in output_str
    assert "\U0001f333  background: TTree" in output_str
    assert "\U0001f333  signal: TTree" in output_str
    assert "   │   ├── branch1:  float64" in output_str
    assert "   │   ├── branch2:  float64" in output_str
    assert "_classname" not in output_str, "internal _classname field should be hidden"


def test_show_structure_save_to_txt(deliver_like_file, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    output_str = file_peeking._show_structure(deliver_like_file)

    # save_to_txt: writes the same string to samples_structure.txt and returns None
    assert file_peeking._show_structure(deliver_like_file, save_to_txt=True) is None
    out_txt = tmp_path / "samples_structure.txt"
    assert out_txt.exists(), "save_to_txt arg not producing samples_structure.txt"
    assert out_txt.read_text(encoding="utf-8") == output_str


def test_show_structure_filter(deliver_like_file):
    filtered_str = file_peeking._show_structure(deliver_like_file, filter="branch2")

    assert "column filter 'branch2'" in filtered_str
    assert "branch1:" not in filtered_str, "filter argument is not removing branch1"
    assert "branch2:" in filtered_str


def test_show_structure_multiple_samples(deliver_like_multi):
    output_str = file_peeking._show_structure(deliver_like_multi)

    assert "\U0001f4c1 Sample: sample_A" in output_str
    assert "\U0001f4c1 Sample: sample_B" in output_str
    assert output_str.index("sample_A") < output_str.index("sample_B")
    assert "background: TTree" in output_str
    assert "jets: TTree" in output_str
    assert "jet_pt:  float64" in output_str


@requires_rntuple_writing
def test_show_structure_rntuple(raw_rntuple_file, tmp_path):
    out_path = _write_delivered(raw_rntuple_file, tmp_path / "delivered_rntuple.root")
    output_str = file_peeking._show_structure({"rntuple_sample": [out_path]})

    assert "\U0001f4c1 Sample: rntuple_sample" in output_str
    assert "\U0001f333  events: ROOT::RNTuple" in output_str
    assert "   │   ├── el_pt:  var * float64" in output_str
    assert "   │   ├── n_el:  int32" in output_str
    assert "_classname" not in output_str


# ---------------------------------------------------------------------------
# _get_arrays
# ---------------------------------------------------------------------------
def test_get_arrays(deliver_like_file):
    arrays = file_peeking._get_arrays(deliver_like_file)

    # Single sample: keyed directly by tree name
    assert isinstance(arrays, dict)
    assert set(arrays.keys()) == {"background", "signal"}
    assert set(arrays["background"].fields) == {"branch1", "branch2", "_classname"}
    assert set(arrays["signal"].fields) == {"branch1", "_classname"}
    assert len(arrays["background"]) == 1
    assert arrays["background"]["branch1"][0] == 1.0


def test_get_arrays_multiple_samples(deliver_like_multi):
    arrays = file_peeking._get_arrays(deliver_like_multi)

    # Multiple samples: keyed by sample name, then tree name
    assert set(arrays.keys()) == {"sample_A", "sample_B"}
    assert arrays["sample_A"] is not arrays["sample_B"]

    # Each sample holds exactly its own trees, nothing leaked from the other
    assert set(arrays["sample_A"].keys()) == {"background", "signal"}
    assert set(arrays["sample_B"].keys()) == {"jets"}
    assert set(arrays["sample_A"]["background"].fields) == {
        "branch1",
        "branch2",
        "_classname",
    }
    assert set(arrays["sample_B"]["jets"].fields) == {"jet_pt", "_classname"}


# ---------------------------------------------------------------------------
# get_structure
# ---------------------------------------------------------------------------
def test_get_structure(deliver_like_file, monkeypatch):
    calls = []

    def fake_deliver(spec):
        calls.append(spec)
        return deliver_like_file

    monkeypatch.setattr(file_peeking, "deliver", fake_deliver)

    output_str = file_peeking.get_structure("random_space:did", filter="branch2")
    assert len(calls) == 1
    assert calls[0]["Sample"][0]["Name"] == "random_space:did"
    assert "branch2:" in output_str and "branch1:" not in output_str

    arrays = file_peeking.get_structure("random_space:did", array_out=True)
    assert set(arrays.keys()) == {"background", "signal"}
