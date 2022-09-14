What is Polyglot Data Manager ?
===============================

.. note::
   Be carefull, this project is not ready for production. It is a Proof Of Concept (POC) for a small project I was working on.
   I figure out that I need a tool like this one. Finally I have decided to make it open source because I did not find any tools that can manage schemas for the NoSQL databases that I use.

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