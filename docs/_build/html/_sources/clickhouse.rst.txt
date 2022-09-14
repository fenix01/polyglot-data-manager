Clickhouse
===========

APIs for Clickhouse : http://127.0.0.1:8016/docs#/clickhouse

Supported types
------------------------

*  int : an integer value.
*  float : a float value.
*  text : an array of character.
*  timestamp : a date supported by the dateparser library (https://pypi.org/project/dateparser/)
*  [int] : a list of integer value
*  [float] : a list of float value
*  [text] : a list of string

Supported index options
------------------------

No options available.

Supported global options
------------------------

*  order_by : a list of fields defined in your schema

Tables are created with this sql pattern : ENGINE = MergeTree() ORDER BY ({}).

Examples
------------------------

.. code-block:: bash

    curl -X 'POST' \
    'http://127.0.0.1:8016/collection/clickhouse' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "collection": "collection1",
    "namespace": "namespace1",
    "fields": {
        "field1": {
            "type": "text"
        },
        "field2": {
            "type":"[int]"
        },
        "field3": {
            "type":"[float]"
        },
        "id": {
            "type":"int"
        }
    },
    "global_options": {
        "order_by": ["id"]
    }
    }'