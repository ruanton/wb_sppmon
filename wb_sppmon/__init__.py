import pyramid.config
import pyramid_zodbconn
import pyramid_tm

# local imports
from . import models


def root_factory(request):
    """ This function is called on every web request
    """
    conn = pyramid_zodbconn.get_connection(request)
    return models.get_app_root(conn)


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    _unused = global_config

    # force explicit transactions
    # see: https://docs.pylonsproject.org/projects/pyramid_tm/en/latest/index.html#custom-transaction-managers
    settings['tm.manager_hook'] = pyramid_tm.explicit_manager

    with pyramid.config.Configurator(settings=settings) as config:
        config.include('pyramid_tm')
        config.include('pyramid_zodbconn')
        config.set_root_factory(root_factory)
    return config.make_wsgi_app()
