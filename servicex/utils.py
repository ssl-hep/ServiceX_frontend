from typing import Optional, Callable


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
