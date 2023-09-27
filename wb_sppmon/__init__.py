from pyramid.config import Configurator
from pyramid_zodbconn import get_connection

# local imports
from .models import appmaker


def root_factory(request):
    """ This function is called on every web request
    """
    conn = get_connection(request)
    return appmaker(conn.root())


def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    _unused = global_config
    with Configurator(settings=settings) as config:
        config.include('pyramid_tm')
        config.include('pyramid_zodbconn')
        config.set_root_factory(root_factory)
    return config.make_wsgi_app()
