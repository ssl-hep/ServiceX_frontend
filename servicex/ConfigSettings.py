from confuse import Configuration, yaml_util, ConfigSource
from pathlib import Path


class ConfigSettings(Configuration):
    def __init__(self, appname, modname=None, loader=yaml_util.Loader):
        Configuration.__init__(self, appname, modname, loader)

    def read(self, user=True, defaults=True):
        '''
        Read in the settings from various locations.
        '''
        if user:
            self._add_local_source()
            self._add_home_source()
            self._add_user_source()
        if defaults:
            self._add_default_source()

    def _add_local_source(self):
        '''
        Look for a '.xxx" file in the local directory
        '''
        p = Path(f'.{self.appname}')
        self._add_from_path(p)

    def _add_from_path(self, p: Path):
        if p.exists():
            yaml_data = yaml_util.load_yaml(p, loader=self.loader) or {}
            self.add(ConfigSource(yaml_data, str(p.resolve())))

    def _add_home_source(self):
        '''
        Look for a '.xxx" file in the local directory
        '''
        self._add_from_path(Path.home() / f'.{self.appname}')
