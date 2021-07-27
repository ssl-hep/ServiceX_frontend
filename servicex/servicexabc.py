from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from make_it_sync import make_sync
import awkward as ak
import pandas as pd

from .utils import (
    DatasetType,
    StatusUpdateFactory,
    _null_progress_feedback,
    _run_default_wrapper,
    _status_update_wrapper,
)


class ServiceXABC(ABC):
    '''
    Abstract base class for accessing the ServiceX front-end for a particular dataset. This does
    have some implementations, but not a full set (hence why it isn't an ABC).

    A light weight, mostly immutable, base class that holds basic configuration information for use
    with ServiceX file access, including the dataset name. Subclasses implement the various access
    methods. Note that not all methods may be accessible!
    '''

    def __init__(self,
                 dataset: DatasetType,
                 image: Optional[str] = None,
                 max_workers: int = 20,
                 status_callback_factory: Optional[StatusUpdateFactory] = _run_default_wrapper,
                 ):
        '''
        Create and configure a ServiceX object for a dataset.

        Arguments

            dataset                     Name of a dataset from which queries will be selected.
            image                       Name of transformer image to use to transform the data. If
                                        None the default implementation is used.
            cache_adaptor               Runs the caching for data and queries that are sent up and
                                        down.
            max_workers                 Maximum number of transformers to run simultaneously on
                                        ServiceX.
            cache_path                  Path to the cache
            status_callback_factory     Factory to create a status notification callback for each
                                        query. One is created per query.


        Notes:

            -  The `status_callback` argument, by default, uses the `tqdm` library to render
               progress bars in a terminal window or a graphic in a Jupyter notebook (with proper
               jupyter extensions installed). If `status_callback` is specified as None, no
               updates will be rendered. A custom callback function can also be specified which
               takes `(total_files, transformed, downloaded, skipped)` as an argument. The
               `total_files` parameter may be `None` until the system knows how many files need to
               be processed (and some files can even be completed before that is known).
        '''
        self._dataset = dataset
        self._image = image
        self._max_workers = max_workers

        # We can't create the notifier until the actual query,
        # so only need to save the status update.
        self._status_callback_factory = \
            status_callback_factory if status_callback_factory is not None \
            else _null_progress_feedback

    def _create_notifier(self, title: Optional[str], downloading: bool) -> _status_update_wrapper:
        'Internal method to create a updater from the status call-back'
        return _status_update_wrapper(self._status_callback_factory(self._dataset, title,
                                                                    downloading))

    @abstractmethod
    async def get_data_rootfiles_async(self, selection_query: str,
                                       title: Optional[str] = None) -> List[Path]:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        a list of root files. The files are uniquely ordered (the same query will always
        return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried
            title               Title reported to the ServiceX backend for status reporting

        Returns:
            root_files          The list of root files
        '''

    @abstractmethod
    async def get_data_pandas_df_async(self, selection_query: str,
                                       title: Optional[str] = None) -> pd.DataFrame:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        a pandas dataframe. The data is uniquely ordered (the same query will always
        return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried
            title               Title reported to the ServiceX backend for status reporting

        Returns:
            df                  The pandas dataframe

        Exceptions:
            xxx                 If the data is not the correct shape (e.g. a flat,
                                rectangular table).
        '''

    @abstractmethod
    async def get_data_awkward_async(self, selection_query: str,
                                     title: Optional[str] = None) \
            -> Dict[bytes, ak.Array]:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        dictionary of awkward arrays, an entry for each column. The data is uniquely
        ordered (the same query will always return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried
            title               Title reported to the ServiceX backend for status reporting

        Returns:
            a                   Dictionary of jagged arrays (as needed), one for each
                                column. The dictionary keys are `bytes` to support possible
                                unicode characters.
        '''

    @abstractmethod
    async def get_data_parquet_async(self, selection_query: str,
                                     title: Optional[str] = None) -> List[Path]:
        '''
        Fetch query data from ServiceX matching `selection_query` and return it as
        a list of parquet files. The files are uniquely ordered (the same query will always
        return the same order).

        Arguments:
            selection_query     The `qastle` string specifying the data to be queried
            title               Title reported to the ServiceX backend for status reporting

        Returns:
            root_files          The list of parquet files
        '''

    # Define the synchronous versions of the async methods for easy of use

    get_data_rootfiles = make_sync(get_data_rootfiles_async)
    get_data_pandas_df = make_sync(get_data_pandas_df_async)
    get_data_awkward = make_sync(get_data_awkward_async)
    get_data_parquet = make_sync(get_data_parquet_async)
