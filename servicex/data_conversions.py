from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import asyncio
from typing import Dict, Union

_conversion_pool = ThreadPoolExecutor(4)


class DataConverterAdaptor:
    '''Methods to convert from one type of data to the other.
    '''
    async def convert_to_pandas(self, file: Path, file_type: str):
        '''Convert to a pandas dataframe from data stored in a file of a particular file_type

        Args:
            file (Path): Path to the file
            file_type (str): What the file contains (root, parquet, etc)
        '''
        if file_type == 'root':
            return await self._convert_root_to_pandas(file)
        elif file_type == 'parquet':
            return await self._convert_parquet_to_pandas(file)
        else:
            raise NotImplementedError(f'Conversion from {file_type} into an pandas DF is not '
                                      'yet supported')

    async def convert_to_awkward(self, file: Path, file_type: str):
        '''Convert to an awkward data array from data stored in a file of a particular file_type

        Args:
            file (Path): Path to the file
            file_type (str): What the file contains (root, parquet, etc)
        '''
        if file_type == 'root':
            return await self._convert_root_to_awkward(file)
        elif file_type == 'parquet':
            return await self._convert_parquet_to_awkward(file)
        else:
            raise NotImplementedError(f'Conversion from {file_type} into an awkward array is not '
                                      'yet supported')

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
            import uproot

            f_in = uproot.open(file)
            try:
                r = f_in[f_in.keys()[0]]
                return r.pandas.df()  # type: ignore
            finally:
                f_in._context.source.close()

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
            - Pandas is only imported if this is called.

        '''
        from numpy import ndarray
        from awkward import JaggedArray

        def do_the_work(file: Path) -> Dict[Union[str, bytes], Union[ndarray, JaggedArray]]:
            import uproot

            f_in = uproot.open(file)
            try:
                r = f_in[f_in.keys()[0]]
                return r.lazyarrays()  # type: ignore
            finally:
                f_in._context.source.close()

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
        import awkward as ak

        def do_the_work(file: Path) -> Dict[Union[str, bytes], ak.ChunkedArray]:
            # TODO: When we move to awkward1, make sure this becomes lazy
            return ak.fromparquet(str(file))

        return await asyncio.wrap_future(_conversion_pool.submit(do_the_work, file))
