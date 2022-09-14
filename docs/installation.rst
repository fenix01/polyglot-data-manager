Installation
============

Ubuntu 22.04
------------------------

Docker & Docker Compose installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We will install docker & docker compose with the latest version. Follow the instructions at this link :

https://docs.docker.com/engine/install/ubuntu/

This project needs access to the docker daemon to execute its tests. It deploys docker containers to test the APIs but it requires admin privileges.
To circumvent this problem, you have to add your user in the docker group.
It will allow to execute docker commands without sudo. Make sure it does not violate your security policy.

.. code::

    sudo usermod -aG docker {your_username}


Execute tests
~~~~~~~~~~~~~

.. code::

    make venv; make tests

Launch the tools
~~~~~~~~~~~~~~~~

.. code::

    docker-compose up