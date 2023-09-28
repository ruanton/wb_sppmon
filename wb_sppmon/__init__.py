from pyramid.config import Configurator
from pyramid_zodbconn import get_connection
from pyramid_tm import explicit_manager

# local imports
from .models import app_root_maker


def root_factory(request):
    """ This function is called on every web request
    """
    conn = get_connection(request)
    return app_root_maker(conn)


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    _unused = global_config

    # force explicit transactions
    # see: https://docs.pylonsproject.org/projects/pyramid_tm/en/latest/index.html#custom-transaction-managers
    settings['tm.manager_hook'] = explicit_manager

    with Configurator(settings=settings) as config:
        config.include('pyramid_tm')
        config.include('pyramid_zodbconn')
        config.set_root_factory(root_factory)
    return config.make_wsgi_app()
