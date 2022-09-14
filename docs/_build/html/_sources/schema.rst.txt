Managing schema
===============

How schema are organized ?
--------------------------

This tool tries to follow the architecture of a relational database.
Data are stored in a database that are stored in a table.
In this tool, the following terminology has been defined :

*  Namespace : a name that defined a group of collections. It is similar to a database.
*  Collection : it stores the data by enforcing a schema. It is similar to a table.
*  Document : an object stored in a Collection that contains attributes.

Some databases follow an architecture with "database" and "table". But some of them does not.
For instance ManticoreSearch does not have the concept of database.
When it is not possible, this tool will prepend namespace name to the collection name. It will use a special caracter like dot "." to seperate the namespace from the collection name.

Generic types supported
------------------------

In this project, all attributes of a schema will be defined with a generic type. Then, this tool will translate the schema in the corresponding format of the database.

Supported types :

*  int : an integer value.
*  float : a float value.
*  text : an array of character.
*  timestamp : a date supported by the dateparser library (https://pypi.org/project/dateparser/)
*  [int] : a list of integer value
*  [float] : a list of float value
*  [text] : a list of string
*  json : a json object
*  relationship : it is a special attribute to connect a Document to other Documents

Moreover, to support features for an attribute or the whole schema we can add options.
These options could be defined :

*  at the attribute level to define an index.
*  for the schema to use a specific engine. For instance clickhouse supports different table engines (MergeTree, Log, etc.).

These options could not be generic and are specifics for each databases.
In the next pages, you may found examples to add schemas for the supported databases.

