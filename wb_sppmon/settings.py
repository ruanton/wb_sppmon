"""
Access to global application settings from the ini-file
"""

import inspect
import pyramid.config


class Settings:
    """Global application settings from config file"""
    def __init__(self):
        self._settings_dict = None

    def init(self, settings_dict: dict[str, str]):
        if self._settings_dict:
            raise RuntimeError(f'_settings_dict is already initialized')
        self._settings_dict = settings_dict

    def _get_int_param(self) -> int:
        param_key = inspect.currentframe().f_back.f_code.co_name  # the calling function name
        if not self._settings_dict:
            raise RuntimeError(f'_settings_dict is not initialized yet')

        try:
            value = int(self._settings_dict[param_key])
            return value
        except Exception as e:
            raise ValueError(f'invalid or misconfigured integer parameter "{param_key}": {e}')

    def _get_str_param(self) -> str:
        param_key = inspect.currentframe().f_back.f_code.co_name  # the calling function name
        if not self._settings_dict:
            raise RuntimeError(f'_settings_dict is not initialized yet')

        try:
            value = self._settings_dict[param_key]
            return value
        except Exception as e:
            raise ValueError(f'invalid or misconfigured string parameter "{param_key}": {e}')

    @property
    def search_min_chars(self) -> int:
        return self._get_int_param()

    @property
    def search_max_suffix(self) -> int:
        return self._get_int_param()


settings = Settings()


def includeme(config: pyramid.config.Configurator):
    settings.init(config.registry.settings)
