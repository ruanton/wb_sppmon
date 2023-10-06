Wildberries SPP Monitor
=======================

Getting Started
---------------

- Change directory into your this project if not already there. Your
  current directory should be the same as this ``README.rst`` file and ``pyproject.toml``.
  Create a Python virtual environment, if not already created::

    python3 -m venv venv

- Upgrade packaging tools::

    ./venv/bin/pip install --upgrade pip setuptools build

- Install and update libraries::

    ./venv/bin/pip install -U -r requirements.txt

- Install the project in editable mode::

    ./venv/bin/pip install -e .

- Create directory for ZODB::

    mkdir zodb-data

- Create configuration files from samples::

    cp config/samples/wb_sppmon.ini.sample config/wb_sppmon.ini
    cp config/samples/admin_emails.lst.sample config/admin_emails.lst
    cp config/samples/report_emails.lst.sample config/report_emails.lst
    cp config/samples/article_numbers.lst.sample config/article_numbers.lst
    cp config/samples/product_categories.lst.sample config/product_categories.lst

- Run project's script::

    ./venv/bin/wb_sppmon config/wb_sppmon.ini

