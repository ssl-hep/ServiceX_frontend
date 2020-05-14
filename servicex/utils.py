from typing import Optional, Callable
from tqdm.auto import tqdm


class _status_update_wrapper:
    '''
    Internal class to make it easier to deal with the updater
    '''
    def __init__(self, callback:
                 Optional[Callable[[Optional[int], int, int, int], None]] = None):
        self._total: Optional[int] = None
        self._processed: Optional[int] = None
        self._downloaded: Optional[int] = None
        self._failed: int = 0
        self._callback = callback

    @property
    def total(self) -> Optional[int]:
        return self._total

    @property
    def failed(self) -> int:
        return self._failed

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
                if self._failed is None:
                    self._failed = 0
                self._callback(self._total, self._processed, self._downloaded, self._failed)

    def update(self, processed: Optional[int] = None,
               downloaded: Optional[int] = None,
               total: Optional[int] = None,
               failed: Optional[int] = None):
        if total is not None:
            if self._total is not None:
                if total > self._total:
                    self._total = total
            else:
                self._total = total
        if processed is not None:
            self._processed = processed
        if downloaded is not None:
            self._downloaded = downloaded
        if failed is not None:
            self._failed = failed

    def inc(self, downloaded: Optional[int] = None):
        if downloaded is not None:
            if self._downloaded is None:
                self._downloaded = downloaded
            else:
                self._downloaded += downloaded


def _run_default_wrapper(t: Optional[int], p: int, d: int, f: int) -> None:
    'Place holder to run the default runner'
    assert False, 'This should never be called'


class _default_wrapper_mgr:
    'Default prorgress bar'
    def __init__(self, sample_name: Optional[str] = None):
        self._tqdm_p = tqdm(total=9e9, desc=sample_name, unit='file',
                            leave=True, dynamic_ncols=True,
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')
        self._tqdm_d = tqdm(total=9e9, desc="        Downloaded", unit='file',
                            leave=True, dynamic_ncols=True,
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')

    def _update_bar(self, bar: tqdm, total: Optional[int], num: int, failed: int):
        if total is not None:
            if bar.total != total:
                # There is a bug in the tqm library if running in a notebook
                # See https://github.com/tqdm/tqdm/issues/688
                # This forces us to do the reset, sadly.
                old_num = bar.n  # type: int
                bar.reset(total)
                bar.update(old_num)
        bar.update(num - bar.n)
        bar.refresh()

        if (failed > 0):
            bar.sp(bar_style='danger')

        if total is not None and (num + failed) == total:
            bar.close()

    def update(self, total: Optional[int], processed: int, downloaded: int, failed: int):
        self._update_bar(self._tqdm_p, total, processed, failed)
        self._update_bar(self._tqdm_d, total, downloaded, failed)
