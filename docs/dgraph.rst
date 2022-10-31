DGraph
===========

APIs for Dgraph : http://127.0.0.1:8016/docs#/dgraph

Supported types
------------------------

*  int : an integer value.
*  float : a float value.
*  text : an array of character.
*  timestamp : a date supported by the dateparser library (https://pypi.org/project/dateparser/)
*  relationship : it is a special attribute to connect a Document to other Documents

Supported index options
------------------------

*  tokenizer : it can take the following value (hash, exact, term, trigram, year, month, day, hour, int, float)

https://dgraph.io/docs/query-language/schema/#indexing

Supported global options
------------------------

No options available.

Examples
------------------------

.. code-block:: bash

    curl -X 'POST' \
    'http://127.0.0.1:8016/collection/dgraph' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
        "collection": "collection1",
        "namespace": "namespace1",
        "fields": {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        },
        "global_options": {
        }
    }'