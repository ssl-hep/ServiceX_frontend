from typing import Optional, Callable
from tqdm.auto import tqdm


class _status_update_wrapper:
    '''
    Internal class to make it easier to deal with the updater
    '''
    def __init__(self, callback:
                 Optional[Callable[[Optional[int], int, int], None]] = None):
        self._total = None
        self._processed = None
        self._downloaded = None
        self._callback = callback

    def broadcast(self):
        'Send an update back to the system'
        if self._callback is not None:
            if self._total is not None \
                    or self._processed is not None \
                    or self._callback is not None:
                if self._processed is None:
                    self._processed = 0
                if self._downloaded is None:
                    self._downloaded = 0
                self._callback(self._total, self._processed, self._downloaded)

    def update(self, processed: Optional[int] = None,
               downloaded: Optional[int] = None,
               total: Optional[int] = None):
        if total is not None:
            self._total = total
        if processed is not None:
            self._processed = processed
        if downloaded is not None:
            self._downloaded = downloaded

    def inc(self, downloaded: Optional[int] = None):
        if downloaded is not None:
            if self._downloaded is None:
                self._downloaded = downloaded
            else:
                self._downloaded += downloaded


def _run_default_wrapper(t: Optional[int], p: int, d: int) -> None:
    'Place holder to run the default runner'
    assert False, 'This should never be called'


class _default_wrapper_mgr:
    'Default prorgress bar'
    def __init__(self, sample_name: Optional[str] = None):
        self._tqdm = tqdm(total=9e9, desc=sample_name, unit='file',
                          leave=True, dynamic_ncols=True,
                          bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')

    def update(self, total: Optional[int], processed: int, downloaded: int):
        if total is not None:
            if self._tqdm.total != total:
                # There is a bug in the tqm library if running in a notebook
                # See https://github.com/tqdm/tqdm/issues/688
                # This forces us to do the reset, sadly.
                old_processed = self._tqdm.n  # type: int
                self._tqdm.reset(total)
                self._tqdm.update(old_processed)
        self._tqdm.update(processed - self._tqdm.n)
        self._tqdm.refresh()

        if total is not None and downloaded == total:
            self._tqdm.close()
