import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import awkward as ak

from .utils import ServiceXException

_conversion_pool = ThreadPoolExecutor(4)


class DataConverterAdaptor:
    '''Methods to convert from one type of data to the other.
    '''
    def __init__(self, default_file_type: str):
        '''Create a data converter adaptor. By default it will do the
        conversation as requested.

        Args:
            default_file_type (str): The default file type (`parquet` or `root`)
        '''
        self._default_file_type = default_file_type

    async def convert_to_pandas(self, file: Path, file_type: Optional[str] = None):
        '''Convert to a pandas dataframe from data stored in a file of a particular file_type

        Args:
            file (Path): Path to the file
            file_type (str): What the file contains (root, parquet, etc)
        '''
        file_type = file_type if file_type is not None else self._default_file_type
        if file_type == 'root':
            return await self._convert_root_to_pandas(file)
        elif file_type == 'parquet':
            return await self._convert_parquet_to_pandas(file)
        else:
            raise ServiceXException(f'Conversion from {file_type} into an pandas DF is not '
                                    'yet supported')

    async def convert_to_awkward(self, file: Path, file_type: Optional[str] = None):
        '''Convert to an awkward data array from data stored in a file of a particular file_type

        Args:
            file (Path): Path to the file
            file_type (str): What the file contains (root, parquet, etc)
        '''
        file_type = file_type if file_type is not None else self._default_file_type
        if file_type == 'root':
            return await self._convert_root_to_awkward(file)
        elif file_type == 'parquet':
            return await self._convert_parquet_to_awkward(file)
        else:
            raise ServiceXException(f'Conversion from {file_type} into an awkward array is not '
                                    'yet supported')

    def combine_pandas(self, dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
        '''Combine many pandas dataframes into a single one, in order.

        Args:
            dfs (Iterable[pd.DataFrame]): The list of dataframes
        '''
        return pd.concat(dfs)

    def combine_awkward(self, awks: Iterable[ak.Array]) -> ak.Array:
        '''Combine many awkward arrays into a single one, in order.

        Args:
            awks (Iterable[ChunkedArray]): The input list of awkward arrays
        '''
        return ak.concatenate(awks)  # type: ignore

    async def _convert_root_to_pandas(self, file: Path):
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
            import uproot as uproot

            with uproot.open(file) as f_in:
                r = f_in[f_in.keys()[0]]
                return r.arrays(library='pd')  # type: ignore

        return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))

    async def _convert_parquet_to_pandas(self, file: Path):
        '''
        Convert the contents of a parquet file to pandas.

        Arguments:

            file        A `Path` to the file containing the pandas data

        Returns:

            DataFrame   A pandas dataframe

        Note:

            - Work is done on a second thread.
            - Pandas is only imported if this is called.

        '''
        import pandas as pd

        def do_the_work(file: Path) -> pd.DataFrame:
            return pd.read_parquet(str(file))

        return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))

    async def _convert_root_to_awkward(self, file: Path):
        '''
        Convert the contents of a ROOT file to an awkward dictionary.

        Arguments:

            file        A `Path` to the file containing the pandas data

        Returns:

            DataFrame   A pandas dataframe

        Note:

            - Work is done on a second thread.
            - Awkward is only imported if this is called.
            - A LazyArray is returned, so it isn't completely loaded into memory. That also means
              this will leak filehandles - as that has to be left open.

        '''
        def do_the_work(file: Path) -> ak.Array:
            import uproot as uproot

            with uproot.open(file) as f_in:
                tree_name = f_in.keys()[0]

            return uproot.lazy(f'{file}:{tree_name}')

        return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))

    async def _convert_parquet_to_awkward(self, file: Path):
        '''
        Convert the contents of a parquet file to an awkward dictionary.

        Arguments:

            file        A `Path` to the file containing the pandas data

        Returns:

            DataFrame   A pandas dataframe

        Note:

            - Work is done on a second thread.
            - Pandas is only imported if this is called.

        '''
        def do_the_work(file: Path) -> ak.Array:
            # TODO: When we move to awkward1, make sure this becomes lazy
            return ak.from_parquet(str(file))  # type: ignore

        return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))
