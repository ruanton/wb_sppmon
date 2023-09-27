Wildberries SPP monitor
=======================

Getting Started
---------------

- Change directory into your this project if not already there. Your
  current directory should be the same as this ``README.rst`` file and ``pyproject.toml``.
  Create a Python virtual environment, if not already created::

    python3 -m venv env

- Upgrade packaging tools::

    ./venv/bin/pip install --upgrade pip setuptools build

- Install and update libraries::

    ./venv/bin/pip install -U -r requirements.txt

- Install the project in editable mode::

    ./venv/bin/pip install -e .

- Create configuration files from samples::

    cp production.ini.sample production.ini

- Create directory for ZODB::

    mkdir zodb-data

- Run project's script::

    ./venv/bin/wb_sppmon production.ini

