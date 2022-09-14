Manticoresearch
===============

APIs for ManticoreSearch : http://127.0.0.1:8016/docs#/manticoresearch

Supported types
------------------------

*  int : an integer value.
*  float : a float value.
*  text : an array of character.
*  timestamp : a date supported by the dateparser library (https://pypi.org/project/dateparser/)
*  json : a json object

Supported index options
------------------------

*  stored : https://manual.manticoresearch.com/Creating_an_index/Data_types#Character-data-types

Supported global options
------------------------

*  min_infix_len : https://manual.manticoresearch.com/Creating_an_index/NLP_and_tokenization/Wildcard_searching_settings#min_prefix_len

Examples
------------------------

.. code-block:: bash

    curl -X 'POST' \
    'http://127.0.0.1:8016/collection/manticoresearch' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "collection": "collection1",
    "namespace": "namespace1",
    "fields": {
        "field1": {
            "type": "text",
            "index": {
                "stored": true
            }
        },
        "field2": {
            "type":"json"
        },
        "id": {
            "type":"int"
        }
    },
    "global_options": {
        "min_infix_len": 3
    }
    }'