SHELL := /bin/bash

venv:
	python3 -m venv env
	( \
       source env/bin/activate; \
       pip3 install -r docker/requirements.txt \
    )
	docker network create --subnet 192.168.101.0/24 net_backend

delete:
	rm -rf env
	docker network rm net_backend

tests:
	( \
       source env/bin/activate; \
       python3 -m pytest \
    )