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
    cp config/samples/contacts_admins.lst.sample config/contacts_admins.lst
    cp config/samples/contacts_users.lst.sample config/contacts_users.lst
    cp config/samples/monitor_articles.lst.sample config/monitor_articles.lst
    cp config/samples/monitor_subcategories.lst.sample config/monitor_subcategories.lst

- Run project's script::

    ./venv/bin/wb_sppmon config/wb_sppmon.ini


- Example bash command to run via cron::

    ps aux | grep -v grep | grep -q wb_sppmon.ini || /root/wb_sppmon/venv/bin/wb_sppmon /root/wb_sppmon/config/wb_sppmon.ini >>/root/wb_sppmon.log 2>&1 &

