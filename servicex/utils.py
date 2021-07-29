from datetime import timedelta
from hashlib import blake2b
import os
from pathlib import Path, PurePath
import re
import tempfile
import threading
from typing import AsyncIterator, Callable, Dict, Iterable, Optional, Tuple, Union

import aiohttp
from confuse.core import ConfigView
from lark import Token, Transformer
from qastle import parse


# Access to thread local storage.
threadLocal = threading.local()


async def default_client_session() -> aiohttp.ClientSession:
    '''
    Return a client session per thread.
    '''
    client = getattr(threadLocal, 'client_session', None)
    if client is None:
        connector = aiohttp.TCPConnector(limit=20)
        client = await aiohttp.ClientSession(connector=connector).__aenter__()
        threadLocal.client_session = client
    return client


def write_query_log(request_id: str, n_files: Optional[int], n_skip: int,
                    time: timedelta, success: bool,
                    path_to_log_dir: Path):
    '''
    Log to a csv file the status of a run.
    '''
    l_file = path_to_log_dir / 'log.csv'
    if l_file.parent.exists():
        if not l_file.exists():
            l_file.write_text('RequestID,n_files,n_skip,time_sec,no_error\n')
        with l_file.open(mode='a') as f:
            s_text = "1" if success else "0"
            f.write(f'{request_id},{n_files if n_files is not None else -1},'
                    f'{n_skip},{time.total_seconds()},{s_text}\n')


class log_adaptor:
    '''
    Helper method to allow easy mocking.
    '''
    @staticmethod
    def write_query_log(request_id: str, n_files: Optional[int], n_skip: int,
                        time: timedelta, success: bool, path_to_log_dir: Path):
        write_query_log(request_id, n_files, n_skip, time, success, path_to_log_dir)


class ServiceXException(Exception):
    'Raised when something has gone wrong in the ServiceX remote service'
    def __init__(self, msg):
        super().__init__(self, msg)


class ServiceXFatalTransformException(Exception):
    'Raised when something has gone wrong in the ServiceX remote service'
    def __init__(self, msg):
        super().__init__(self, msg)


class ServiceXUnknownRequestID(Exception):
    'Raised when we try to access ServiceX with a request ID it does not know about'
    def __init__(self, msg):
        super().__init__(self, msg)


class ServiceXUnknownDataRequestID(Exception):
    '''Raised when we expect ServiceX to know about the data for a request id, but it does not.
    Possible causes:

      - ServiceX has been reset and all previously run requests have been removed
      - ServiceX's cache rolled over this entry, and it wasn't cached locally.
    '''
    def __init__(self, msg):
        super().__init__(self, msg)


class ServiceXFailedFileTransform(Exception):
    'Raised when a file(s) fail to transform'
    def __init__(self, msg):
        super().__init__(self, msg)


def sanitize_filename(fname: str):
    'No matter the string given, make it an acceptable filename'
    return fname.replace('*', '_') \
                .replace(';', '_') \
                .replace(':', '_')


# Datasets can be specified in mulitple ways
# to ServiceX. This datatype encapuslates them
# all.
DatasetType = Union[
    str,  # A single URI with a scheme (e.g. rucio://did_name, http://, root://)
    Iterable[str]  # A list of http:// or root:// files to be directly accessed
]

StatusUpdateCallback = Callable[[Optional[int], int, int, int], None]
# The sig of the call-back. Arguments are:
# 1. Processed
# 1. Downloaded
# 1. Remaining
# 1. Failed

StatusUpdateFactory = Callable[[DatasetType, Optional[str], bool], StatusUpdateCallback]
# Factory method that returns a factory that can run the status callback
# First argument is a string, second argument is True if a download progress
# bar can be displayed as well.


class _status_update_wrapper:
    '''
    Internal class to make it easier to deal with the updater
    '''
    def __init__(self, callback:
                 Optional[StatusUpdateCallback] = None):
        self._callback = callback
        self.reset()

    def reset(self):
        'Reset back to zero'
        self._total: Optional[int] = None
        self._processed: Optional[int] = None
        self._downloaded: Optional[int] = None
        self._failed: int = 0
        self._remaining: Optional[int] = None

    @property
    def total(self) -> Optional[int]:
        return self._total

    @property
    def failed(self) -> int:
        return self._failed

    def broadcast(self):
        'Send an update back to the system'
        if self._callback is not None:
            processed = self._processed if self._processed is not None else 0
            downloaded = self._downloaded if self._downloaded is not None else 0
            failed = self._failed if self._failed is not None else 0
            self._callback(self._total, processed, downloaded, failed)

    def _update_total(self):
        'Update total number of files, without letting inconsistencies change things'
        if self._processed is not None \
                and self._remaining is not None \
                and self._failed is not None:
            total = self._remaining + self._processed + self._failed
            if self._total is None:
                self._total = total
            else:
                if self._total < total:
                    self._total = total

    def update(self, processed: Optional[int] = None,
               downloaded: Optional[int] = None,
               remaining: Optional[int] = None,
               failed: Optional[int] = None):
        if processed is not None:
            self._processed = processed
        if downloaded is not None:
            self._downloaded = downloaded
        if failed is not None:
            self._failed = failed
        if remaining is not None:
            self._remaining = remaining
        self._update_total()

    def inc(self, downloaded: Optional[int] = None):
        if downloaded is not None:
            if self._downloaded is None:
                self._downloaded = downloaded
            else:
                self._downloaded += downloaded


TransformTuple = Tuple[Optional[int], int, Optional[int]]


async def stream_status_updates(stream: AsyncIterator[TransformTuple],
                                notifier: _status_update_wrapper):
    '''
    As the transformed status goes by, update the notifier with the new
    values.
    '''
    async for p in stream:
        remaining, processed, failed = p
        notifier.update(processed=processed, failed=failed, remaining=remaining)
        notifier.broadcast()
        yield p


async def stream_unique_updates_only(stream: AsyncIterator[TransformTuple]):
    '''
    As status goes by, only let through changes
    '''
    last_p: Optional[TransformTuple] = None
    async for p in stream:
        if p != last_p:
            last_p = p
            yield p


def _run_default_wrapper(ds_name: DatasetType, title: Optional[str],
                         downloading: bool) -> StatusUpdateCallback:
    '''Create a feedback object for everyone to use to pass feedback to. Uses tqdm (default).

    Args:
        ds_name (str): The dataset name
        downloading (bool): True if we will be downloading the files, not just processing them.

    Returns:
        StatusUpdateCallback: The updater callback
    '''
    return _default_wrapper_mgr(ds_name, title, downloading).update


def _null_progress_feedback(ds_name: DatasetType, title: Optional[str], downloading: bool) -> None:
    '''
    Internal routine to create a feedback object that does not
    give anyone feedback!
    '''
    return None


def _bar_name(sample_name: Optional[DatasetType], title: Optional[str]) -> str:
    """Returns a name that is suitable for a progress bar

    Args:
        sample_name (str): The sample info
        title (Optional[str]): The title specified with the request

    Returns:
        str: Name to use for a progress bar
    """
    if title is not None:
        name = title
    else:
        if sample_name is not None:
            name = sample_name if isinstance(sample_name, str) \
                else f"[{', '.join(sample_name)}]"
        else:
            name = '<none>'

    if len(name) > 20:
        name = name[0:20] + '...'

    return name


# Hack to work around tqdm not understanding vscode notebooks (see #186).
if 'VSCODE_PID' in os.environ.keys():  # pragma: no cover
    from tqdm.notebook import tqdm
else:
    from tqdm.auto import tqdm


class _default_wrapper_mgr:
    'Default progress bar'
    def __init__(self, sample_name: Optional[DatasetType] = None, title: str = None,
                 show_download_bar: bool = True):
        self._tqdm_p: Optional[tqdm] = None
        self._tqdm_d: Optional[tqdm] = None
        self._show_download_progress = show_download_bar
        self._name = _bar_name(sample_name, title)

    def _init_tqdm(self):
        if self._tqdm_p is not None:
            return

        self._tqdm_p = tqdm(total=9e9, desc=self._name, unit='file',
                            leave=False, dynamic_ncols=True,
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')
        if self._show_download_progress:
            self._tqdm_d = tqdm(total=9e9, desc=f"        {self._name} Downloaded", unit='file',
                                leave=False, dynamic_ncols=True,
                                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]')

    def _update_bar(self, bar: Optional[tqdm], total: Optional[int], num: int, failed: int):
        assert bar is not None, 'Internal error - bar was not initalized'
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

        if total is not None and (num + failed) == total:
            bar.close()

    def update(self, total: Optional[int], processed: int, downloaded: int, failed: int):
        self._init_tqdm()
        self._update_bar(self._tqdm_p, total, processed, failed)
        if self._show_download_progress:
            self._update_bar(self._tqdm_d, total, downloaded, failed)


# Changes in the json that won't affect the result
_json_keys_to_ignore_for_hash = ['workers']


def _query_cache_hash(json_query: Dict[str, str]) -> str:
    '''
    Return a query cache file.
    '''
    hasher = blake2b(digest_size=20)
    for k, v in json_query.items():
        if k not in _json_keys_to_ignore_for_hash:
            if k == 'selection':
                v = clean_linq(v)
            hasher.update(k.encode())
            hasher.update(str(v).encode())
    hash = hasher.hexdigest()
    return hash


def _string_hash(s_list: Iterable[Union[str, Iterable[str]]]) -> str:
    '''
    Return a hash for an input list of strings.
    '''
    hasher = blake2b(digest_size=20)
    for v in s_list:
        if isinstance(v, str):
            hasher.update(v.encode())
        else:
            for v1 in v:
                hasher.update(v1.encode())
    hash = hasher.hexdigest()
    return hash


def clean_linq(q: str) -> str:
    '''
    Normalize the variables in a qastle expression. Should make the
    linq expression more easily comparable even if the algorithm that
    generates the underlying variable numbers changes. If the selection algorithm will violate
    the qastle syntax, it is returned unaltered and with no errors.

    Arguments

        q           Strign containing the qastle code`

    Returns

        clean_q     Sanitized query - lambda arguments are given a uniform source code
                    labeling so that two queries with the same structure are marked as the same.
    '''
    from collections import namedtuple
    ParseTracker = namedtuple('ParseTracker', ['text', 'info'])

    arg_index = 0

    def new_arg():
        nonlocal arg_index
        s = f'a{arg_index}'
        arg_index += 1
        return s

    def _replace_strings(s: str, rep: Dict[str, str]) -> str:
        'Replace all strings in dict that are found in s'
        for k in rep.keys():
            s = re.sub(f'\\b{k}\\b', rep[k], s)
        return s

    class translator(Transformer):
        def record(self, children):
            if (len(children) == 0
                    or isinstance(children[0], Token)
                    and children[0].type == 'WHITESPACE'):  # type: ignore
                return ""
            else:
                return children[0].text  # type: ignore

        def expression(self, children):
            for child in children:
                if isinstance(child, ParseTracker):
                    return child
            raise SyntaxError('Expression does not contain a node')

        def atom(self, children):
            child = children[0]
            return ParseTracker(child.value, child.value)

        def composite(self, children):
            fields = []
            node_type: Optional[str] = None
            for child in children:
                if isinstance(child, Token):
                    if child.type == 'NODE_TYPE':  # type: ignore
                        node_type = child.value  # type: ignore
                elif isinstance(child, ParseTracker):
                    fields.append(child)

            assert node_type is not None

            if node_type == 'lambda':
                if len(fields) != 2:
                    raise ServiceXException(
                        f'The qastle "{q}" is not valid - found a lambda '
                        f'expression with {len(fields)} arguments - not the required 2!')
                arg_list = [f.info for f in fields[0].info]
                arg_mapping = {old: new_arg() for old in arg_list}
                fields[0] = ParseTracker(
                    f'(list {" ".join(arg_mapping[k] for k in arg_list)})',
                    [ParseTracker(arg_mapping[f], arg_mapping[f]) for f in arg_list])
                fields[1] = ParseTracker(_replace_strings(fields[1].text, arg_mapping),
                                         fields[1].info)

            return ParseTracker(f'({node_type} {" ".join([f.text for f in fields])})', fields)

    try:
        tree = parse(q)
        new_tree = translator().transform(tree)
        assert isinstance(new_tree, str)
        return new_tree
    except Exception:
        return q


def get_configured_cache_path(config: ConfigView) -> Path:
    '''
    From the configuration info return a valid path for us to store our cached files.
    '''
    assert config["cache_path"].exists(), 'Cannot find default config - install is broken'
    s_path = config["cache_path"].as_str_expanded()
    assert isinstance(s_path, str)

    # If they have tried to use the USER or UserName as an expansion, and it has failed, then
    # translate it to maintain harmony across platforms.
    if '${USER}' in s_path and 'UserName' in os.environ:
        s_path = s_path.replace('${USER}', os.environ['UserName'])
    if '${UserName}' in s_path and 'USER' in os.environ:
        s_path = s_path.replace('${UserName}', os.environ['USER'])

    p_p = PurePath(s_path)
    if len(p_p.parts) > 1 and p_p.parts[1] == 'tmp':
        p = Path(tempfile.gettempdir()) / Path(*p_p.parts[2:])
    else:
        p = Path(p_p)
    p.mkdir(exist_ok=True, parents=True)
    return p

# Below is copied and very lightly modified from the backoff library
# See https://github.com/litl/backoff/issues/131


from backoff import full_jitter  # noqa: E402
from backoff._common import (  # noqa: E402
    _prepare_logger,
    _config_handlers,
    _log_giveup,
    _log_backoff,
    _maybe_call,
    _init_wait_gen,
    _next_wait,
)
import logging  # noqa: E402
from backoff._async import (  # noqa: E402
    _ensure_coroutines,
    _ensure_coroutine,
    _call_handlers,
)
import asyncio  # noqa: E402
import functools  # noqa: E402
import datetime  # noqa: E402


def retry_exception_itr(target, wait_gen, exception,
                        max_tries, max_time, jitter, giveup,
                        on_success, on_backoff, on_giveup,
                        wait_gen_kwargs):
    on_success = _ensure_coroutines(on_success)
    on_backoff = _ensure_coroutines(on_backoff)
    on_giveup = _ensure_coroutines(on_giveup)
    giveup = _ensure_coroutine(giveup)

    # Easy to implement, please report if you need this.
    assert not asyncio.iscoroutinefunction(max_tries)
    assert not asyncio.iscoroutinefunction(jitter)

    @functools.wraps(target)
    async def retry(*args, **kwargs):
        # change names because python 2.x doesn't have nonlocal
        max_tries_ = _maybe_call(max_tries)
        max_time_ = _maybe_call(max_time)

        tries = 0
        start = datetime.datetime.now()
        wait = _init_wait_gen(wait_gen, wait_gen_kwargs)
        while True:
            tries += 1
            elapsed = timedelta.total_seconds(datetime.datetime.now() - start)
            details = (target, args, kwargs, tries, elapsed)

            got_one_item = False
            try:
                async for item in target(*args, **kwargs):
                    got_one_item = True
                    yield item
            except exception as e:
                # If we've already fed a result out of this method,
                # we can't pull it back. So don't try to pull back/retry
                # the exception either.
                if got_one_item:
                    raise

                giveup_result = await giveup(e)
                max_tries_exceeded = (tries == max_tries_)
                max_time_exceeded = (max_time_ is not None
                                     and elapsed >= max_time_)

                if giveup_result or max_tries_exceeded or max_time_exceeded:
                    await _call_handlers(on_giveup, *details)
                    raise

                try:
                    seconds = _next_wait(wait, jitter, elapsed, max_time_)
                except StopIteration:  # pragma: no cover
                    await _call_handlers(on_giveup, *details)
                    raise e

                await _call_handlers(on_backoff, *details, wait=seconds)

                # Note: there is no convenient way to pass explicit event
                # loop to decorator, so here we assume that either default
                # thread event loop is set and correct (it mostly is
                # by default), or Python >= 3.5.3 or Python >= 3.6 is used
                # where loop.get_event_loop() in coroutine guaranteed to
                # return correct value.
                # See for details:
                #   <https://groups.google.com/forum/#!topic/python-tulip/yF9C-rFpiKk>
                #   <https://bugs.python.org/issue28613>
                await asyncio.sleep(seconds)
            else:
                await _call_handlers(on_success, *details)
                return
    return retry


def on_exception_itr(wait_gen,
                     exception,
                     max_tries=None,
                     max_time=None,
                     jitter=full_jitter,
                     giveup=lambda e: False,
                     on_success=None,
                     on_backoff=None,
                     on_giveup=None,
                     logger='backoff',
                     backoff_log_level=logging.INFO,
                     giveup_log_level=logging.ERROR,
                     **wait_gen_kwargs):
    def decorate(target):
        # change names because python 2.x doesn't have nonlocal
        logger_ = _prepare_logger(logger)

        on_success_ = _config_handlers(on_success)
        on_backoff_ = _config_handlers(
            on_backoff, _log_backoff, logger_, backoff_log_level
        )
        on_giveup_ = _config_handlers(
            on_giveup, _log_giveup, logger_, giveup_log_level
        )

        return retry_exception_itr(target, wait_gen, exception,
                                   max_tries, max_time, jitter, giveup,
                                   on_success_, on_backoff_, on_giveup_,
                                   wait_gen_kwargs)

    # Return a function which decorates a target with a retry loop.
    return decorate
