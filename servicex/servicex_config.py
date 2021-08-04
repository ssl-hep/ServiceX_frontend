import logging
from typing import Optional, Tuple, cast

from confuse.core import ConfigView

from servicex.ConfigSettings import ConfigSettings
from .utils import ServiceXException


class ServiceXConfigAdaptor:
    '''Contains the logic to extract all the configuration information needed, as driven from
    things the user has given us and program parameters. This concentrates all the complex config
    logic in on place, and, hopefully, leaves the logic out of all the other code.
    '''
    def __init__(self, config: Optional[ConfigView] = None):
        '''The config needed for the app.

        Note: The config is held onto and only queired when the information is required.

        Args:
            config (ConfigView): The config information for the app. If null, then we just use the
                                 standard servicex name.
        '''
        self._settings = config if config is not None else ConfigSettings('servicex', 'servicex')

    @property
    def settings(self) -> ConfigView:
        '''Return the config settings.

        Eventually this should not be used other than for testing!

        Returns:
            ConfigSettings: The config settings that gives us a view on everything the user wants
        '''
        return self._settings

    def get_default_returned_datatype(self, backend_name: Optional[str]) -> str:
        '''Return the default return data type, given the backend is a certian type.

        Args:
            backend_name (Optional[str]): The backend type string (`xaod`, `uproot`, etc)

        Returns:
            str: The backend datatype, like `root` or `parquet`.
        '''
        # Check to see if we know about the backend info
        if backend_name is not None:
            info = self.get_backend_info(backend_name, 'return_data')
            if info is not None:
                return info

        if self._settings['default_return_data'].exists():
            return cast(str, self._settings['default_return_data'].as_str_expanded())

        raise ServiceXException('A default default_return_data is missing from config files - is '
                                'servicex installed correctly?')

    def get_backend_info(self, backend_name: str, key: str) -> Optional[str]:
        '''Find an item in the backend info, searching first the backend settings and then
           searching the defaults.

        Args:
            backend_name (str): Backend name
            key (str): The key for the info we are after

        Returns:
            Optional[str]: Return a string for the info we are after, or return None if we can't
                           find it.
        '''

        def find_in_list(c, key) -> Optional[str]:
            if c.exists():
                for ep in c:
                    if ep['type'].exists():
                        if ep['type'].as_str() == backend_name:
                            if key in ep:
                                return ep[key].as_str_expanded()
            return None

        a = find_in_list(self._settings['api_endpoints'], key)
        a = find_in_list(self._settings['backend_types'], key) if a is None else a
        return a

    def get_servicex_adaptor_config(self, backend_name: Optional[str] = None) -> \
            Tuple[str, Optional[str]]:
        '''Return the servicex (endpoint, username, email) from a given backend configuration.

        Args:
            backend_name (str): The backend name (like `xaod`) which we hopefully can find in the
            configuration file.

        Returns:
            Tuple[str, str]: The tuple of info to create a `ServiceXAdaptor`: end point,
            token (optionally).
        '''
        # Find a list of all endpoints.
        # It is an error if this is not specified somewhere.
        endpoints = self._settings['api_endpoints']

        def extract_info(ep) -> Tuple[str, Optional[str]]:
            endpoint = ep['endpoint'].as_str_expanded()
            token = ep['token'].as_str_expanded() if 'token' in ep else None

            # We can default these to "None"
            return (endpoint, token)  # type: ignore

        # If we have a good name, look for exact match
        if backend_name is not None:
            for ep in endpoints:
                if ep['name'].exists() and ep['name'].as_str_expanded() == backend_name:
                    return extract_info(ep)
            for ep in endpoints:
                if ep['type'].exists() \
                        and not ep['name'].exists() \
                        and ep['type'].as_str_expanded() == backend_name:
                    return extract_info(ep)
            seen_types = [str(ep['type'].as_str_expanded()) for ep in endpoints
                          if ep['type'].exists()]
            raise ServiceXException(f'Unable to find type {backend_name} '
                                    'in servicex.yaml configuration file. Saw only types'
                                    f': {", ".join(seen_types)}')

        # See if one is unlabeled.
        log = logging.getLogger(__name__)
        for ep in endpoints:
            if not ep['type'].exists():
                log.warning('No backend type requested, '
                            f'using {ep["endpoint"].as_str_expanded()} - please be explicit '
                            'in the ServiceXDataset constructor')
                return extract_info(ep)

        # Nope - now we are going to have to just use the first one there.
        for ep in endpoints:
            log.warning('No backend type requested, '
                        f'using {ep["endpoint"].as_str_expanded()} - please be explicit '
                        'in the ServiceXDataset constructor')
            return extract_info(ep)

        # If we are here - then... it just isn't going to work!
        raise ServiceXException('Not even a default set of configurations are here! Bad install '
                                ' of the servicex package!')
