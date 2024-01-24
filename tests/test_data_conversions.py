import shutil
from servicex import ServiceXException
from servicex.data_conversions import DataConverterAdaptor
import pytest
import pandas as pd
import awkward as ak


def check_awkward_accessible(col: ak.Array):
    "Check to make sure we can look at every item in column"
    ak.sum(col)


def check_pandas_accessible(col):
    assert len(col.array) > 0


@pytest.mark.asyncio
async def test_root_to_pandas(good_root_file_path):
    df = await DataConverterAdaptor("root-file").convert_to_pandas(good_root_file_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 283458
    check_pandas_accessible(df["JetPt"])


@pytest.mark.asyncio
async def test_root_to_pandas_default(good_root_file_path):
    df = await DataConverterAdaptor("root-file").convert_to_pandas(
        good_root_file_path, "root-file"
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 283458


@pytest.mark.asyncio
async def test_parquet_to_pandas_non_default(good_uproot_file_path):
    df = await DataConverterAdaptor("root-file").convert_to_pandas(
        good_uproot_file_path, "parquet"
    )
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 115714


@pytest.mark.asyncio
async def test_parquet_to_pandas(good_uproot_file_path):
    df = await DataConverterAdaptor("parquet").convert_to_pandas(good_uproot_file_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 115714
    check_pandas_accessible(df["JetPT"])


@pytest.mark.asyncio
async def test_parquet_to_awkward(good_uproot_file_path):
    df = await DataConverterAdaptor("parquet").convert_to_awkward(good_uproot_file_path)
    assert len(df["JetPT"]) == 115714  # type: ignore
    check_awkward_accessible(df["JetPT"])  # type: ignore


@pytest.mark.asyncio
async def test_root_to_awkward(good_root_file_path):
    df = await DataConverterAdaptor("root-file").convert_to_awkward(good_root_file_path)
    assert len(df["JetPt"]) == 283458  # type: ignore
    check_awkward_accessible(df["JetPt"])  # type: ignore


@pytest.mark.asyncio
async def test_root_to_awkward_postfix_filename(good_root_file_path, tmp_path):
    # Found in the wild see #337.
    postfix_file = tmp_path / "test.root.1"
    shutil.copy(good_root_file_path, postfix_file)

    df = await DataConverterAdaptor("root-file").convert_to_awkward(postfix_file)
    assert len(df["JetPt"]) == 283458  # type: ignore
    check_awkward_accessible(df["JetPt"])  # type: ignore


@pytest.mark.asyncio
async def test_to_awkward_fail(good_root_file_path):
    with pytest.raises(ServiceXException):
        await DataConverterAdaptor("root-file").convert_to_awkward(
            good_root_file_path, "not-really"
        )


@pytest.mark.asyncio
async def test_to_pandas_fail(good_root_file_path):
    with pytest.raises(ServiceXException):
        await DataConverterAdaptor("root-file").convert_to_pandas(
            good_root_file_path, "not-really"
        )


def test_combine_pandas_from_root(good_root_file_path):
    "Load a DataFrame from root files and make sure that they work when we ask them to combine"

    def load_df():
        import uproot as uproot

        with uproot.open(good_root_file_path) as f_in:  # type: ignore
            r = f_in[f_in.keys()[0]]
            return r.arrays(library="pd")  # type: ignore

    df1 = load_df()
    df2 = load_df()

    combined = DataConverterAdaptor("root-file").combine_pandas([df1, df2])

    assert len(combined) == len(df1) + len(df2)
    check_pandas_accessible(combined["JetPt"])


def test_combine_pandas_from_parquet(good_uproot_file_path):
    "Load a DataFrame from a parquet file and make sure they work when we ask them to combine"

    def load_df():
        import pandas as pd

        return pd.read_parquet(good_uproot_file_path)

    df1 = load_df()
    df2 = load_df()

    combined = DataConverterAdaptor("root-file").combine_pandas([df1, df2])

    assert len(combined) == len(df1) + len(df2)
    check_pandas_accessible(combined["JetPT"])


def test_combine_awkward_from_root(good_root_file_path):
    "Load a DataFrame from root files and make sure that they work when we ask them to combine"

    def load_df():
        import uproot as uproot

        with uproot.open(good_root_file_path) as f_in:  # type: ignore
            tree_name = f_in.keys()[0]
            return f_in[tree_name].arrays()  # type: ignore

    df1 = load_df()
    df2 = load_df()

    combined = DataConverterAdaptor("root-file").combine_awkward([df1, df2])

    assert len(combined) == len(df1) + len(df2)
    check_awkward_accessible(combined["JetPt"])  # type: ignore


def test_combine_awkward_from_parquet(good_uproot_file_path):
    "Load a DataFrame from a parquet file and make sure they work when we ask them to combine"

    def load_df():
        return ak.from_parquet(good_uproot_file_path)  # type: ignore

    df1 = load_df()
    df2 = load_df()

    combined = DataConverterAdaptor("root-file").combine_awkward([df1, df2])  # type: ignore

    assert len(combined) == len(df1) + len(df2)  # type: ignore
    check_awkward_accessible(combined["JetPT"])  # type: ignore
