# Polyglot Data Manager

## Description

The goal of this tool is to help manage schemas for various SQL/NoSQL databases with only one REST API.
It will abstract the creation and provisioning of schemas and offers write API by enforcing a schema.
In fact, in the database ecosystem there is as many query languages as databases. Adding more and more databases to your project will increase dramatically the maintenance and the management of your data model.

This tool will put an abstraction layer over your databases and provides the following features :

*  Centralize all schemas in one tools
*  One syntax to create all schemas for various databases
*  An API to write data and enforce the schema


    ![Architecture of polyglot data manager](docs/schemas/archi.png)

    

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

*  Graph databases :
    * Dgraph <https://dgraph.io/>
    * ArangoDB <https://www.arangodb.com/>
*  SearchEngine :
    * ManticoreSearch <https://manticoresearch.com/>
*  Column oriented databases / analytics :
    * Clickhouse <https://clickhouse.com/>



## Documentation

The documentation has been written with Sphinx.
https://polyglot-data-manager.readthedocs.io/en/latest/

## Usage

The docker image has been deployed on Dockerhub. You can follow this docker-compose example :

```
version : "2.4"
services:
  polyglot:
    image: fenix011/polyglot-data-manager:latest
    depends_on:
      - kvrocks
      - manticore
      - clickhouse
      - arangodb
    ports:
      - "127.0.0.1:8016:8016"
    environment:
      - NODE_TYPE=chief
      - REST_PORT=8016
      - KVROCKS_HOSTNAME=kvrocks
      - KVROCKS_PORT=6666
      - CLICKHOUSE_HOSTNAME=clickhouse
      - CLICKHOUSE_PORT=8123
      - ARANGODB_HOSTNAME=arangodb
      - ARANGODB_PORT=8529
      - ARANGODB_USER=root
      - ARANGODB_PASSWORD=CHANGEME
      - MANTICORESEARCH_HOSTNAME=manticore
      - MANTICORESEARCH_PORT=9308
      - CONNECTORS=manticoresearch,clickhouse,arangodb
  kvrocks:
    image: 'apache/kvrocks:2.2.0'
    ports:
      - 127.0.0.1:6666:6666
  arangodb:
    image: arangodb/arangodb:3.9.2
    environment:
      - ARANGO_ROOT_PASSWORD=CHANGEME
    ports:
      - '127.0.0.1:8529:8529'
  manticore:
    image: manticoresearch/manticore:4.0.2
    ports:
      - 127.0.0.1:9306:9306
      - 127.0.0.1:9308:9308
    ulimits:
      nproc: 65535
      nofile:
         soft: 65535
         hard: 65535
      memlock:
        soft: -1
        hard: -1
  clickhouse:
    image: yandex/clickhouse-server:21.6.4-alpine
    ports:
      - 127.0.0.1:9000:9000
      - 127.0.0.1:8123:8123
```

## Rebuild

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