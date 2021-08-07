import logging
from typing import Dict, Optional, Tuple

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
        r = self.get_backend_info(backend_name, 'return_data')

        if r is None:
            raise ServiceXException('A default default_return_data is missing from config files '
                                    '- is servicex installed correctly?')

        return r

    def get_backend_info(self, backend_name: Optional[str], key: str) -> Optional[str]:
        '''Find an item in the backend info, searching first for the backend
        name/type and then the defaults with a given type.

        Args:
            backend_name (str): Backend name
            key (str): The key for the info we are after

        Returns:
            Optional[str]: Return a string for the info we are after, or return None if we can't
                           find it.
        '''
        config = self._get_backend_info(backend_name)
        return config[key] if key in config else None

    def _get_backend_info(self, backend_name: Optional[str]) -> Dict[str, str]:
        '''Returns all the info for a backend name/type.

        Search algoirthm is non-trivial:
        1. If `backend_name` is not `None`:
           1. Look at the `api_endpoints` for a `name` matching `backend_name`.
           2. Look at the `api_endpoints` for a `type` matching `backend_name`,
              complain that `name` wasn't present.
           3. Fail if nothing matches
        2. If `backend_name` is None:
           1. Use the first end point in the list, and complain.

        Given the above is done, then look at `backend_types` for a matching `type`,
        and for any key found there not already present, add it, and return the dictionary.

        Args:
            backend_name (str): Name or type of the api end point we are going to look up.

        Returns:
            Dict[str, str]: Attributes for this backend's configuration
        '''
        # Find a list of all endpoints.
        # It is an error if this is not specified somewhere.
        endpoints = self.settings['api_endpoints']

        # If we have a good name, look for exact match
        config: Optional[Dict[str, str]] = None
        log = logging.getLogger(__name__)
        if backend_name is not None:
            # Search the list of end points for a matching name
            for ep in endpoints:
                if ep['name'].exists() and ep['name'].as_str_expanded() == backend_name:
                    config = {k: str(ep[k].as_str_expanded()) for k in ep.keys()}
                    break
            if config is None:
                for ep in endpoints:
                    if ep['type'].exists() \
                            and ep['type'].as_str_expanded() == backend_name:
                        config = {k: str(ep[k].as_str_expanded()) for k in ep.keys()}
                        log.warning(f'Found backend type matching "{backend_name}". Matching by '
                                    'type is depreciated. Please switch to using the "name" '
                                    'keyword in your servicex.yaml file.')
                        break
            if config is None:
                # They asked for something explicit, we couldn't find it, so we need to bomb.
                # Given how annoying this is, build a useful error message.
                seen_types = [str(ep['type'].as_str_expanded()) for ep in endpoints
                              if ep['type'].exists()]
                seen_names = [str(ep['name'].as_str_expanded()) for ep in endpoints
                              if ep['name'].exists()]

                raise ServiceXException(f'Unable to find name/type {backend_name} '
                                        'in api_endpoints in servicex.yaml configuration file. Saw'
                                        f' only names ({", ".join(seen_names)}) and types '
                                        f'({", ".join(seen_types)})')

        # Nope - now we are going to have to just use the first one there.
        else:
            for ep in endpoints:
                log.warning('No backend name/type requested, '
                            f'using {ep["endpoint"].as_str_expanded()} - please be explicit '
                            'in the ServiceXDataset constructor')
                config = {k: str(ep[k].as_str_expanded()) for k in ep.keys()}
                break

        if config is None:
            # If we are here - then... it just isn't going to work!
            raise ServiceXException('Not even a default set of configurations are here! Bad '
                                    ' install of the servicex package!')

        # Now, extract the type and see if we can figure out any defaults from the
        # `backend_types` info.
        type_lookup = config['type'] if 'type' in config else backend_name
        if type_lookup is None:
            return config

        backend_defaults = self.settings['backend_types']
        for bd in backend_defaults:
            if bd['type'].as_str_expanded() == type_lookup:
                for k in bd.keys():
                    if k not in config:
                        config[k] = str(bd[k].as_str_expanded())

        # Finally, a default return type
        if 'return_data' not in config:
            if 'default_return_data' in self.settings.keys():
                config['return_data'] = \
                    str(self.settings['default_return_data'].as_str_expanded())

        return config

    def get_servicex_adaptor_config(self, backend_name: Optional[str] = None) -> \
            Tuple[str, Optional[str]]:
        '''Return the servicex (endpoint, token) from a given backend configuration.

        Args:
            backend_name (str): The backend name (like `xaod`) which we hopefully can find in the
            configuration file.

        Returns:
            Tuple[str, str]: The tuple of info to create a `ServiceXAdaptor`: end point,
            token (optionally).
        '''
        config = self._get_backend_info(backend_name)

        endpoint = config['endpoint']
        token = config['token'] if 'token' in config else None

        # We can default these to "None"
        return (endpoint, token)  # type: ignore
