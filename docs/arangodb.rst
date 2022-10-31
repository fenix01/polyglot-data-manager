Arangodb
===========

APIs for Arangodb : http://127.0.0.1:8016/docs#/arangodb

This API makes it possible to use collection and anonymous graph with the help of edge collection.

Supported types
------------------------

*  int : an integer value.
*  float : a float value.
*  text : an array of character.
*  timestamp : a date supported by the dateparser library (https://pypi.org/project/dateparser/)
*  relationship : it is a special attribute to connect a Document to other Documents
*  [int] : a list of integer value
*  [float] : a list of float value
*  [text] : a list of string
*  json : a json object

Supported index options
------------------------

Indexes in Arangodb are defined at the collection level (global options).

Supported global options
------------------------

*  indexes : a list of index defined with a "name", "index_type" and "fields"

  *  index_type : it supports the following index "persistent", "fulltext", "hash"
  *  name : it would be the index name in Arangodb
  *  fields : the list of fields for this index

*  edge_collection : a boolean that defined if this collection is for edge documents.


Examples
------------------------

.. code-block:: bash

    curl -X 'POST' \
    'http://127.0.0.1:8016/collection/arangodb' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
            "collection": "collection1",
            "namespace": "namespace1",
            "global_options": {
                "indexes":[
                    {
                        "index_type": "fulltext",
                        "name": "test_index",
                        "fields": ["test"]
                    }
                ],
                "edge_collection": false
            },
            "fields":
            {
                "test": {
                    "type": "text"
                },
                "test2": {
                    "type": "text"
                }
            }
        }'

How to create an annonymous graph ?
------------------------------------

First create a collection :

.. code-block:: bash

    curl -X 'POST' \
    'http://127.0.0.1:8016/collection/arangodb' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
            "collection": "collection1",
            "namespace": "namespace1",
            "global_options": {
                "indexes":[],
                "edge_collection": false
            },
            "fields":
            {
                "test": {
                    "type": "text"
                }
            }
        }'

Then create the edge collection. We need to set edge_collection to true and add two fields "from" and "to" of type "relationship":

.. code-block:: bash

    curl -X 'POST' \
    'http://127.0.0.1:8016/collection/arangodb' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
            "collection": "collection2",
            "namespace": "namespace1",
            "global_options": {
                "indexes":[],
                "edge_collection": true
            },
            "fields":
            {
                "from": {
                    "type": "relationship"
                },
                "to": {
                    "type": "relationship"
                },
                "label": {
                    "type": "text"
                }
            }
        }'

After these steps, you can add nodes and create relationships :

.. code-block:: python3

    import requests
    import json

    response1 = requests.post("http://127.0.0.1:8016/collection/arangodb/nodes", json={
        "collection": "collection1",
        "namespace": "test",
        "nodes":[
            {
                "test": "test"
            },
            {
                "test": "test2"
            }
        ]
        })
    nodes_id = json.loads(response1.text)["nodes_id"]
    response2 = requests.put("http://127.0.0.1:8016/collection/arangodb/relationship", json={
    "collection": "collection2",
    "namespace": "test",
    "edge": {
            "from": nodes_id[0]["_id"],
            "to": nodes_id[1]["_id"],
            "label": "label"
        }
    })