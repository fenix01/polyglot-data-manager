SHELL := /bin/bash

venv:
	python3 -m venv env
	( \
       source env/bin/activate; \
       pip3 install -r docker/requirements.txt \
    )

delete:
	rm -rf env

tests:
	( \
       source env/bin/activate; \
       python3 -m pytest \
    )