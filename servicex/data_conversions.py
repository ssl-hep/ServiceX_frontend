from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import Dict, Union

_conversion_pool = ThreadPoolExecutor(2)


async def _convert_root_to_pandas(file: Path):
    '''
    Convert the contents of a ROOT file to pandas.

    Arguments:

        file        A `Path` to the file containing the pandas data

    Returns:

        DataFrame   A pandas dataframe

    Note:

        - Work is done on a second thread.
        - Pandas is only imported if this is called.

    '''
    from pandas import DataFrame

    def do_the_work(file: Path) -> DataFrame:
        import uproot

        f_in = uproot.open(file)
        try:
            r = f_in[f_in.keys()[0]]
            return r.pandas.df()  # type: ignore
        finally:
            f_in._context.source.close()

    return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))


async def _convert_root_to_awkward(file: Path):
    '''
    Convert the contents of a ROOT file to an awkward dictionary.

    Arguments:

        file        A `Path` to the file containing the pandas data

    Returns:

        DataFrame   A pandas dataframe

    Note:

        - Work is done on a second thread.
        - Pandas is only imported if this is called.

    '''
    from numpy import ndarray
    from awkward import JaggedArray

    def do_the_work(file: Path) -> Dict[bytes, Union[ndarray, JaggedArray]]:
        import uproot

        f_in = uproot.open(file)
        try:
            r = f_in[f_in.keys()[0]]
            return r.arrays()  # type: ignore
        finally:
            f_in._context.source.close()

    return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))
