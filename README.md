# Polyglot Data Manager

## Description

The goal of this tool is to help manage schemas for various SQL/NoSQL databases with only one REST API.
It will abstract the creation and provisioning of schemas and offer write API by enforcing a schema.
In fact, in the database ecosystem there is as many query languages as databases. Adding more and more databases to your project will increase dramatically the maintenance and the management of your data model.

This tool will put an abstraction layer over your databases and provide the following features :

*  Centralize all schemas in one tools
*  One syntax to create all schemas for various databases
*  An API to write data and enforce the schema


Regarding read access to data, you just need to connect to your database and make a query. This tool will not handle this part, it is not its objective.

What this tools is not :

*  It does not provide a new query language for reading.
*  It is not an ETL/ELT
*  It does not have any intelligency. Most works will be supported by your database


Today, it exists many kind of databases :

*  Relational databases : MySQL, MariaDB, Oracle, PostgreSQL, ...
*  Graph databases : Neo4J, DGraph, TypeDB, ArangoDB ...
*  SearchEngine : Opensearch, Elasticsearch, ManticoreSearch, Quickwit, ...
*  Document oriented databases : MongoDB, Oracle, ...
*  Column oriented databases / analytics : Clickhouse, BigTable
*  Time based databases : Prometheus, VictoriaMetrics, InfluxDB, Grafana Mimir

This tools supports the following database :

*  Graph databases : `Dgraph <https://dgraph.io/>`_
*  SearchEngine : `ManticoreSearch <https://dgraph.io/>`_
*  Column oriented databases / analytics : `Clickhouse <https://clickhouse.com/>`_

## Documentation

The documentation has been written with Sphinx. The html doc is located in docs/_build/html/index.html

## Usage

Dependencies :
- Docker
- Docker Compose
- Python 3.8

We will install docker & docker compose with the latest version. Follow the instructions at this link :

https://docs.docker.com/engine/install/ubuntu/

This project needs access to the docker daemon to execute its tests. It deploys docker containers to test the APIs but it requires admin privileges.
To circumvent this problem, you have to add your user in the docker group.
It will allow to execute docker commands without sudo. Make sure it does not violate your security policy.

```bash
sudo usermod -aG docker {your_username}
```
    
To run all tests :
```bash
make venv ; make tests
```


To run the project calls :
```bash
sudo docker-compose up
```

## License

MIT