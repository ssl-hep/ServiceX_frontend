# Copyright (c) 2022, IRIS-HEP
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
from servicex.dataset_identifier import (
    CERNOpenDataDatasetIdentifier,
    DataSetIdentifier,
    FileListDataset,
    RucioDatasetIdentifier,
    XRootDDatasetIdentifier,
)
import pytest


def test_did():
    did = DataSetIdentifier(scheme="rucio", dataset="abc:123-455")
    assert did.did == "rucio://abc:123-455"


def test_rucio():
    did = RucioDatasetIdentifier("abc:123-456")
    assert did.did == "rucio://abc:123-456"
    assert did.describe() == "rucio://abc:123-456"


def test_rucio_with_file_limit():
    did = RucioDatasetIdentifier("abc:123-456", num_files=10)
    assert did.describe() == "rucio://abc:123-456?files=10"


def test_rucio_no_namespace():
    with pytest.raises(ValueError):
        RucioDatasetIdentifier("123-456")


def test_file_list():
    did = FileListDataset(["c:/foo.bar"])
    assert did.files == ["c:/foo.bar"]
    assert did.describe() == "1 file: c:/foo.bar"


def test_single_file():
    did = FileListDataset("c:/foo.bar")
    assert did.files == ["c:/foo.bar"]
    assert did.describe() == "1 file: c:/foo.bar"


def test_file_list_multiple_preview():
    did = FileListDataset(["file1.root", "file2.root", "file3.root"])

    assert did.describe() == "3 files: file1.root, file2.root, file3.root"


def test_file_list_ellipsis_for_many_files():
    files = ["file1.root", "file2.root", "file3.root", "file4.root"]
    did = FileListDataset(files)

    assert did.describe() == "4 files: file1.root, file2.root, file3.root, ..."


def test_populate_transform_request(transform_request):
    did = FileListDataset(["c:/foo.bar"])
    did.populate_transform_request(transform_request)
    assert transform_request.file_list == ["c:/foo.bar"]

    did2 = RucioDatasetIdentifier("abc:123-456")
    did2.populate_transform_request(transform_request)
    assert transform_request.did == "rucio://abc:123-456"


def test_cern_open_data_description():
    did = CERNOpenDataDatasetIdentifier(12345)
    assert did.describe() == "cernopendata://12345"


def test_cern_open_data_description_with_file_limit():
    did = CERNOpenDataDatasetIdentifier(12345, num_files=2)
    assert did.describe() == "cernopendata://12345?files=2"


def test_xrootd_description():
    did = XRootDDatasetIdentifier("root://server//data/*.root")
    assert did.describe() == "xrootd://root://server//data/*.root"


def test_xrootd_description_with_file_limit():
    did = XRootDDatasetIdentifier("root://server//data/*.root", num_files=1)
    assert did.describe() == "xrootd://root://server//data/*.root?files=1"
