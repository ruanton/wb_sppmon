"""
Access to global application settings from the ini-file
"""

import inspect
import pyramid.config
import decimal


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

    def _get_decimal_param(self) -> decimal.Decimal:
        param_key = inspect.currentframe().f_back.f_code.co_name  # the calling function name
        if not self._settings_dict:
            raise RuntimeError(f'_settings_dict is not initialized yet')

        try:
            value = decimal.Decimal(str(self._settings_dict[param_key]))
            return value
        except Exception as e:
            raise ValueError(f'invalid or misconfigured decimal parameter "{param_key}": {e}')

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
    def contacts_admins_file(self) -> str:
        """File with contacts of administrators where to send script errors"""
        return self._get_str_param()

    @property
    def contacts_users_file(self) -> str:
        """File with contacts of users where to send reports"""
        return self._get_str_param()

    @property
    def telegram_bot_token(self) -> str:
        """Telegram bot API token"""
        return self._get_str_param()

    @property
    def report_errors_delay_interval(self) -> int:
        """Report errors for the same entity no more often than one per this number of minutes"""
        return self._get_int_param()

    @property
    def report_changes_delay_interval(self) -> int:
        """Report changes for the same entity no more often than one per this number of minutes"""
        return self._get_int_param()

    @property
    def monitor_articles_file(self) -> str:
        """File with WB article numbers to monitor"""
        return self._get_str_param()

    @property
    def monitor_subcategories_file(self) -> str:
        """File with WB product categories to monitor"""
        return self._get_str_param()

    @property
    def max_matched_subcategories(self) -> int:
        """If matched more subcategories for any input category, reject all those subcategories"""
        return self._get_int_param()

    @property
    def search_min_chars(self) -> int:
        """Minimum number of characters suitable for imprecise text searching"""
        return self._get_int_param()

    @property
    def search_max_suffix(self) -> int:
        """Maximum length of non-matching suffix"""
        return self._get_int_param()

    @property
    def http_retries(self) -> int:
        """Default number of HTTP request retries"""
        return self._get_int_param()

    @property
    def http_base_retry_pause(self) -> decimal.Decimal:
        """Default base of random pause between retries of failed HTTP requests"""
        return self._get_decimal_param()

    @property
    def products_num_pages_to_fetch(self) -> int:
        """Number of product listing pages to fetch for each search criterion"""
        return self._get_int_param()

    @property
    def products_num_to_determine_spp(self) -> int:
        """Minimum number of products for reliable determination of SPP"""
        return self._get_int_param()

    @property
    def products_num_percent_min_determine_spp(self) -> decimal.Decimal:
        """Minimum percentage of products with the same SPP to reliable determination of SPP"""
        return self._get_decimal_param()

    @property
    def maximum_total_discount_base(self) -> decimal.Decimal:
        """Maximal total discount to start from (not used)"""
        return self._get_decimal_param()

    @property
    def maximum_client_discount_base(self) -> decimal.Decimal:
        """Maximal client discount"""
        return self._get_decimal_param()


settings = Settings()


def includeme(config: pyramid.config.Configurator):
    settings.init(config.registry.settings)
