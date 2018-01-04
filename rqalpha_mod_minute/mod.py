from rqalpha.interface import AbstractMod

from .data_source import MinuteDataSource


class MinuteMod(AbstractMod):
    def __init__(self):
        pass

    def start_up(self, env, mod_config):
        bundle_path = env.config.base.data_bundle_path
        env.set_data_source(MinuteDataSource(bundle_path))

    def tear_down(self, code, exception=None):
        pass