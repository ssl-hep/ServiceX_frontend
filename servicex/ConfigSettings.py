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
        self._add_from_path(Path(f'{self.appname}.yaml'), walk_up_tree=True)
        self._add_from_path(Path(f'{self.appname}.yml'), walk_up_tree=True)
        self._add_from_path(Path(f'.{self.appname}'), walk_up_tree=True)

    # def _add_from_path(self, p: Path):
    #     if p.exists():
    #         yaml_data = yaml_util.load_yaml(p, loader=self.loader) or {}
    #         self.add(ConfigSource(yaml_data, str(p.resolve())))

    def _add_from_path(self, p: Path, walk_up_tree: bool = False):
        p.resolve()
        name = p.name
        dir = p.parent.resolve()
        while True:
            f = dir / name
            if f.exists():
                yaml_data = yaml_util.load_yaml(f, loader=self.loader) or {}
                self.add(ConfigSource(yaml_data, str(f)))
                break
            if not walk_up_tree:
                break
            if dir == dir.parent:
                break
            dir = dir.parent

    def _add_home_source(self):
        '''
        Look for a '.xxx" file in the local directory
        '''
        self._add_from_path(Path.home() / f'{self.appname}.yaml')
        self._add_from_path(Path.home() / f'{self.appname}.yml')
        self._add_from_path(Path.home() / f'.{self.appname}')
