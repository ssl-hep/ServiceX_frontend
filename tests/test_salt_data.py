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
import base64
import types
from unittest.mock import patch

import pytest

from servicex.dataset_identifier import FileListDataset, RucioDatasetIdentifier
from servicex.servicex_client import GuardList
from servicex.tools import salt_data

np = pytest.importorskip("numpy")
ak = pytest.importorskip("awkward")
uproot = pytest.importorskip("uproot")
h5py = pytest.importorskip("h5py")


CONFIG = {
    "tree_name": "reco",
    "cuts": [
        "num(jet_pt[jet_pt > 25000]) > 1",
        "num(el_pt) > 0",
    ],
    "objects": {
        "event": {"branches": {"met": "met"}, "attach_label": True},
        "jet": {"branches": {"pt": "jet_pt", "eta": "jet_eta"}},
        "el": {"branches": {"pt": "el_pt"}},
    },
    "output_file": "out.h5",
    "max_events": 5,
}

SAMPLES = {
    "sig": {"dataset": "user.me:sig.ntuple", "label": 1, "nfiles": 2},
    "bkg": {"files": ["root://host//a.root", "root://host//b.root"], "label": 0},
}


def jagged(lists, dtype="f4"):
    """Typed jagged array (also when empty)."""
    flat = np.asarray([x for sub in lists for x in sub], dtype=dtype)
    return ak.unflatten(flat, np.asarray([len(sub) for sub in lists], dtype=np.int64))


def make_root_file(path, jets, electrons, met=None, tree="reco"):
    """ROOT file with jagged jet/electron branches (lists of pT per event)."""
    n = len(jets)
    data = {
        "jet_pt": jagged(jets),
        "jet_eta": jagged([[0.1 * i for i in range(len(j))] for j in jets]),
        "el_pt": jagged(electrons),
        "met": np.asarray(met if met is not None else np.arange(n), dtype="f4"),
        "other": np.zeros(n, dtype="f4"),
    }
    with uproot.recreate(path) as f:
        f[tree] = data
    return str(path)


def load_transformer(config, label):
    """The query source exactly as sent to ServiceX, executed as a module."""
    query = salt_data.build_salt_query(config, label)
    source = base64.b64decode(query.generate_selection_string()).decode("ascii")
    module = types.ModuleType("generated_transformer")
    exec(compile(source, "generated_transformer.py", "exec"), module.__dict__)
    return module


def read_h5(path):
    with h5py.File(path, "r") as f:
        return {k: f[k][:] for k in f}, dict(f.attrs)


# --- configuration ---------------------------------------------------------


def test_load_salt_config_from_yaml(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "tree_name: reco\ncuts: 'num(jet_pt) > 0'\n"
        "objects:\n  event:\n    branches: {met: met}\n    attach_label: true\n"
    )
    config = salt_data.load_salt_config(path)
    assert config["tree_name"] == "reco"
    assert config["cuts"] == ["num(jet_pt) > 0"]


@pytest.mark.parametrize(
    "change, message",
    [
        ({"tree_name": None}, "tree_name"),
        ({"objects": {}}, "objects"),
        ({"objects": {"jet": {"branches": {}}}}, "branches"),
        ({"objects": {"jet": {"branches": {"pt": "jet_pt"}}}}, "attach_label"),
    ],
)
def test_load_salt_config_invalid(change, message):
    with pytest.raises(ValueError, match=message):
        salt_data.load_salt_config({**CONFIG, **change})


def test_load_salt_samples_wrapper_and_validation(tmp_path):
    path = tmp_path / "samples.yaml"
    path.write_text("samples:\n  a:\n    dataset: user.me:a\n    label: 1\n")
    assert salt_data.load_salt_samples(path) == {
        "a": {"dataset": "user.me:a", "label": 1}
    }


@pytest.mark.parametrize(
    "sample, message",
    [
        ({"dataset": "d"}, "label"),
        ({"dataset": "d", "label": True}, "label"),
        ({"label": 0}, "either"),
        ({"dataset": "d", "files": ["f"], "label": 0}, "either"),
    ],
)
def test_load_salt_samples_invalid(sample, message):
    with pytest.raises(ValueError, match=message):
        salt_data.load_salt_samples({"s": sample})


def test_load_salt_samples_empty():
    with pytest.raises(ValueError, match="no samples"):
        salt_data.load_salt_samples({})


# --- query and spec --------------------------------------------------------


def test_build_salt_query_injects_config_and_label():
    query = salt_data.build_salt_query(CONFIG, 1)
    source = base64.b64decode(query.generate_selection_string()).decode("ascii")
    assert source.startswith("SALT_CONFIG = ")
    assert "SALT_LABEL = 1\n" in source
    assert "def run_query(input_filenames, output_path):" in source
    module = load_transformer(CONFIG, 0)
    assert module.SALT_LABEL == 0
    # client-only keys are not sent to the transformers
    assert set(module.SALT_CONFIG) == {"tree_name", "cuts", "objects"}


def test_build_salt_query_rejects_non_ascii():
    config = {**CONFIG, "cuts": ["num(jet_pt) > 0  # \u00e9"]}
    with pytest.raises(ValueError, match="ASCII"):
        salt_data.build_salt_query(config, 1)


def test_build_salt_spec():
    spec = salt_data.build_salt_spec(CONFIG, SAMPLES, delivery="URLs")
    assert spec["General"]["Delivery"] == "URLs"
    sig, bkg = spec["Sample"]
    assert isinstance(sig["Dataset"], RucioDatasetIdentifier)
    assert sig["Dataset"].num_files == 2
    assert isinstance(bkg["Dataset"], FileListDataset)
    assert bkg["Dataset"].files == ["root://host//a.root", "root://host//b.root"]
    assert sig["Query"] is not bkg["Query"]  # one query per label


def test_build_salt_spec_reuses_query_per_label():
    samples = {
        "a": {"dataset": "user.me:d1", "label": 0},
        "b": {"dataset": "user.me:d2", "label": 0},
    }
    a, b = salt_data.build_salt_spec(CONFIG, samples)["Sample"]
    assert a["Query"] is b["Query"]


# --- transformer function --------------------------------------------------

JETS = [[30e3, 40e3], [30e3], [50e3, 30e3, 20e3, 26e3], [10e3, 30e3, 30e3], []]
ELS = [[20e3], [20e3], [5e3, 6e3], [], [1e3]]
# passing events: 0 (2 jets, 1 el) and 2 (3 jets > 25 GeV, 2 el)


def test_run_query_cuts_and_automatic_padding(tmp_path):
    path = make_root_file(tmp_path / "in.root", JETS, ELS)
    out = tmp_path / "out.h5"
    load_transformer(CONFIG, 1).run_query(path, str(out))

    data, attrs = read_h5(out)
    assert attrs["n_events_in"] == 5
    assert attrs["n_events"] == 2
    assert attrs["label"] == 1
    assert list(data["event"]["met"]) == [0, 2]
    assert list(data["event"]["label"]) == [1, 1]
    # padded to the largest multiplicity in this file: 4 jets, 2 electrons
    assert data["jet"].shape == (2, 4)
    assert data["el"].shape == (2, 2)
    assert data["jet"]["valid"].tolist() == [[True, True, False, False], [True] * 4]
    assert data["jet"]["pt"][0].tolist() == [30e3, 40e3, 0, 0]
    assert data["el"]["valid"].tolist() == [[True, False], [True, True]]


def test_run_query_chunks_match_single_pass(tmp_path):
    path = make_root_file(tmp_path / "in.root", JETS * 7, ELS * 7)
    single, chunked = tmp_path / "single.h5", tmp_path / "chunked.h5"
    load_transformer(CONFIG, 0).run_query(path, str(single))
    load_transformer({**CONFIG, "step_size": 3}, 0).run_query(path, str(chunked))

    a, _ = read_h5(single)
    b, _ = read_h5(chunked)
    for obj in a:
        assert a[obj].shape == b[obj].shape
        assert a[obj].tobytes() == b[obj].tobytes()


def test_run_query_without_cuts(tmp_path):
    path = make_root_file(tmp_path / "in.root", JETS, ELS)
    out = tmp_path / "out.h5"
    load_transformer({**CONFIG, "cuts": []}, 0).run_query(path, str(out))
    data, attrs = read_h5(out)
    assert attrs["n_events"] == 5
    assert data["jet"].shape == (5, 4)


def test_run_query_cut_functions(tmp_path):
    path = make_root_file(tmp_path / "in.root", JETS, ELS)
    out = tmp_path / "out.h5"
    cuts = ["any(jet_pt > 45000) | (sum(el_pt) > 30000)", "abs(met) >= 0"]
    load_transformer({**CONFIG, "cuts": cuts}, 0).run_query(path, str(out))
    data, _ = read_h5(out)
    assert list(data["event"]["met"]) == [2]


def test_run_query_no_event_passes(tmp_path):
    path = make_root_file(tmp_path / "in.root", JETS, ELS)
    out = tmp_path / "out.h5"
    load_transformer({**CONFIG, "cuts": ["met < 0"]}, 0).run_query(path, str(out))
    data, attrs = read_h5(out)
    assert attrs["n_events"] == 0
    assert data["event"].shape == (0,)
    assert data["jet"].shape == (0, 0)
    assert "valid" in data["jet"].dtype.names


def test_run_query_empty_tree(tmp_path):
    path = make_root_file(tmp_path / "in.root", [], [], met=[])
    out = tmp_path / "out.h5"
    load_transformer(CONFIG, 0).run_query(path, str(out))
    data, attrs = read_h5(out)
    assert attrs["n_events_in"] == 0
    assert set(data) == {"event", "jet", "el"}


def test_run_query_missing_branch(tmp_path):
    path = make_root_file(tmp_path / "in.root", JETS, ELS)
    config = {
        **CONFIG,
        "objects": {**CONFIG["objects"], "mu": {"branches": {"pt": "mu_pt"}}},
    }
    with pytest.raises(ValueError, match="mu_pt"):
        load_transformer(config, 0).run_query(path, str(tmp_path / "out.h5"))


# --- merge -----------------------------------------------------------------


def transformed(tmp_path, name, jets, electrons, label, config=CONFIG):
    path = make_root_file(tmp_path / f"{name}.root", jets, electrons, met=None)
    out = tmp_path / f"{name}.root.parquet"
    load_transformer({**config, "cuts": []}, label).run_query(path, str(out))
    return str(out)


def test_merge_common_width(tmp_path):
    files = {
        "sig": [
            transformed(tmp_path, "sig._000002", [[1.0] * 6], [[1.0]], 1),
            transformed(tmp_path, "sig._000001", [[1.0, 2.0]], [[1.0, 2.0, 3.0]], 1),
        ],
        "bkg": [transformed(tmp_path, "bkg._000001", [[3.0, 4.0, 5.0]], [[]], 0)],
    }
    summary = salt_data.merge_salt_files(files, tmp_path / "merged.h5")
    assert summary["n_events"] == 3
    assert summary["widths"] == {"jet": 6, "el": 3}
    assert summary["events_per_sample"] == {"sig": 2, "bkg": 1}

    data, _ = read_h5(tmp_path / "merged.h5")
    assert data["jet"].shape == (3, 6)
    # files of a sample are merged in input-file order (_000001 before _000002)
    assert data["jet"]["valid"].sum(axis=1).tolist() == [2, 6, 3]
    assert data["jet"]["pt"][0].tolist() == [1.0, 2.0, 0, 0, 0, 0]
    assert data["el"]["valid"].sum(axis=1).tolist() == [3, 1, 0]
    assert data["event"]["label"].tolist() == [1, 1, 0]


def test_merge_max_events_trims_width(tmp_path):
    files = {
        "sig": [
            transformed(tmp_path, "sig._000001", [[1.0], [1.0] * 5], [[1.0], [1.0]], 1),
            transformed(tmp_path, "sig._000002", [[1.0] * 7], [[1.0]], 1),
        ]
    }
    summary = salt_data.merge_salt_files(files, tmp_path / "merged.h5", max_events=1)
    assert summary["n_events"] == 1
    # the file is 5 jets wide, but the only kept event has 1 jet: width trimmed to 1
    assert summary["widths"]["jet"] == 1
    data, _ = read_h5(tmp_path / "merged.h5")
    assert data["jet"].shape == (1, 1)


def test_merge_promotes_dtypes(tmp_path):
    a = transformed(tmp_path, "a._000001", [[1.0]], [[1.0]], 1)
    b = transformed(tmp_path, "b._000001", [[2.0]], [[2.0]], 0)
    with h5py.File(b, "r+") as f:  # store the second file's met as float64
        event = f["event"][:]
        del f["event"]
        f["event"] = event.astype([("met", "<f8"), ("label", "<i8")])
    salt_data.merge_salt_files({"a": [a], "b": [b]}, tmp_path / "merged.h5")
    data, _ = read_h5(tmp_path / "merged.h5")
    assert data["event"].dtype["met"] == np.dtype("<f8")


def test_merge_inconsistent_fields(tmp_path):
    a = transformed(tmp_path, "a._000001", [[1.0]], [[1.0]], 1)
    b = transformed(tmp_path, "b._000001", [[2.0]], [[2.0]], 0)
    with h5py.File(b, "r+") as f:
        event = f["event"][:]
        del f["event"]
        f["event"] = event[["met"]].astype([("met", "<f4")])
    with pytest.raises(ValueError, match="inconsistent fields"):
        salt_data.merge_salt_files({"a": [a], "b": [b]}, tmp_path / "merged.h5")


def test_merge_no_events(tmp_path):
    empty = transformed(tmp_path, "e._000001", [], [], 0)
    with pytest.raises(ValueError, match="no events"):
        salt_data.merge_salt_files({"e": [empty]}, tmp_path / "merged.h5")


def test_merge_downloads_urls(tmp_path):
    local = transformed(tmp_path, "sig._000001", [[1.0, 2.0]], [[1.0]], 1)
    url = "https://store/bucket/sig._000001.root.parquet?X-Amz-Signature=abc"

    def fake_download(u, directory):
        assert u == url
        return local

    with patch.object(salt_data, "_download", side_effect=fake_download) as download:
        summary = salt_data.merge_salt_files({"sig": [url]}, tmp_path / "merged.h5")
    download.assert_called_once()
    assert summary["n_events"] == 1


def test_file_key_strips_url_query():
    assert (
        salt_data._file_key("https://h/b/x._000002.parquet?sig=1")
        == "x._000002.parquet"
    )
    assert salt_data._file_key("/tmp/cache/x._000001.parquet") == "x._000001.parquet"


# --- deliver and get_salt_data ---------------------------------------------


def test_deliver_salt_files_skips_failed_samples():
    results = {
        "ok": GuardList(["/tmp/a.parquet"]),
        "failed": GuardList(ValueError("transform failed")),
        "empty": GuardList([]),
    }
    samples = {name: {"dataset": "user.me:d", "label": 0} for name in results}
    with patch.object(salt_data, "deliver", return_value=results) as deliver:
        files = salt_data.deliver_salt_files(CONFIG, samples, servicex_name="test")
    assert files == {"ok": ["/tmp/a.parquet"]}
    spec = deliver.call_args.args[0]
    assert [s["Name"] for s in spec["Sample"]] == ["ok", "failed", "empty"]
    assert deliver.call_args.kwargs["servicex_name"] == "test"


def test_get_salt_data_defaults_from_config():
    files = {"sig": ["/tmp/a.parquet"]}
    merge_summary = {
        "output_file": "out.h5",
        "n_events": 5,
        "events_per_sample": {"sig": 5},
        "widths": {},
        "merge_seconds": 0.1,
    }
    with (
        patch.object(salt_data, "deliver_salt_files", return_value=files) as deliver,
        patch.object(
            salt_data, "merge_salt_files", return_value=merge_summary
        ) as merge,
    ):
        summary = salt_data.get_salt_data(CONFIG, SAMPLES, delivery="URLs")
    merge.assert_called_once_with(files, "out.h5", max_events=5)
    assert deliver.call_args.kwargs["delivery"] == "URLs"
    assert summary["failed_samples"] == ["bkg"]
    assert "servicex_seconds" in summary


def test_get_salt_data_nothing_delivered():
    with patch.object(salt_data, "deliver_salt_files", return_value={}):
        with pytest.raises(RuntimeError, match="no sample"):
            salt_data.get_salt_data(CONFIG, SAMPLES)
