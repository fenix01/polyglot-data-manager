from polymanager.schemas.arangodb.arangodb_schema import ArangodbSchema
import pytest

class TestArangodbSchema():
    def test_invalid_type(self):
        with pytest.raises(Exception):
            ArangodbSchema("test", "test", {"a": {"type": "double"}})

    def test_invalid_type2(self):
        with pytest.raises(Exception):
            ArangodbSchema("test", "test", {"a": {"type": "unknown"}})

    def test_valid_type(self):
        ArangodbSchema("test", "test", 
        {
            "a": {"type": "int"},
            "b": {"type": "float"},
            "c": {"type": "text"},
            "d": {"type": "timestamp"},
            "e": {"type": "relationship"},
            "f": {"type": "[int]"},
            "g": {"type": "[float]"},
            "h": {"type": "[text]"},
            "i": {"type": "json"},
            })

    def test_schema(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "text"}})
        assert arangodb_schema.get_schema() == {'fields': {'a': {'type': 'text'}}, 'collection': 'test', 'namespace': 'test'}

    def test_fields(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "text"}})
        assert arangodb_schema.get_fields() == {'a': {'type': 'text'}}

    def test_collection_name(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "text"}})
        assert arangodb_schema.get_collection_name() == "test"

    def test_collection_name2(self):
        arangodb_schema = ArangodbSchema( "test.test", "test", {"a": {"type": "text"}})
        assert arangodb_schema.get_collection_name() == "test"

    def test_node_schema(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "text"}})
        with pytest.raises(Exception):
            arangodb_schema.check_node_schema({"z": ""})

    def test_node_schema2(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "json"}})
        arangodb_schema.check_node_schema({"a": '{"attr1": ["val1"]}'})

    def test_node_schema3(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "json"}})
        arangodb_schema.check_node_schema({"a": '{"attr1": "val1"}'})

    def test_get_node_schema(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "timestamp"},"b": {"type":"relationship"}})
        assert arangodb_schema.get_node_schema({"a": "2021/01/01"}) == {'a': '2021-01-01T00:00:00'}

    def test_relationship_schema(self):
        with pytest.raises(Exception):
            arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "text"},"b": {"type":"relationship"}})
            arangodb_schema.check_relationship_schema({"a": ""})
        
    def test_relationship_fields(self):
        arangodb_schema = ArangodbSchema("test", "test", {"a": {"type": "text"},"b": {"type":"relationship"}})
        assert arangodb_schema.get_relationship_fields() == ["a", "b"]

    def test_invalid_indexes_type(self):
        fields = {
            "a": {
                "type": "text"
            },
            "b": {
                "type":"relationship"
            }
        }
        global_collection_opts = {
            "indexes": [
                {
                    "index_type": "unknown",
                    "name": "test",
                    "fields": ["a"]
                }
            ]
        }
        with pytest.raises(Exception):
            ArangodbSchema("test", "test", fields, global_collection_opts=global_collection_opts)

    def test_valid_indexes(self):
        fields = {
            "a": {
                "type": "text"
            },
            "b": {
                "type":"int"
            }
        }
        global_collection_opts = {
            "indexes": [
                {
                    "index_type": "fulltext",
                    "name": "test",
                    "fields": ["a"]
                }
            ],
            "edge_collection": False
        }
        ArangodbSchema("test", "test", fields, global_collection_opts=global_collection_opts)

    def test_invalid_edge_collection(self):
        fields = {
            "a": {
                "type": "text"
            },
            "b": {
                "type":"relationship"
            }
        }
        global_collection_opts = {
            "indexes": [
                {
                    "index_type": "fulltext",
                    "name": "test",
                    "fields": ["a"]
                }
            ],
            "edge_collection": True
        }
        with pytest.raises(Exception):
            ArangodbSchema("test", "test", fields, global_collection_opts=global_collection_opts)
        

    def test_invalid_edge_collection2(self):
        fields = {
            "_from": {
                "type": "text"
            }
        }
        global_collection_opts = {
            "indexes": [
                {
                    "index_type": "fulltext",
                    "name": "test",
                    "fields": ["a"]
                }
            ],
            "edge_collection": True
        }
        with pytest.raises(Exception):
            ArangodbSchema("test", "test", fields, global_collection_opts=global_collection_opts)

    def test_valid_edge_collection(self):
        fields = {
            "from": {
                "type": "relationship"
            },
            "to": {
                "type": "relationship"
            }
        }
        global_collection_opts = {
            "indexes": [],
            "edge_collection": True
        }
        ArangodbSchema("test", "test", fields, global_collection_opts=global_collection_opts)

    def test_invalid_relationship_field(self):
        fields = {
            "from": {
                "type": "relationship"
            },
            "to": {
                "type": "relationship"
            }
        }
        global_collection_opts = {
            "indexes": [],
            "edge_collection": False
        }
        with pytest.raises(Exception):
            ArangodbSchema("test", "test", fields, global_collection_opts=global_collection_opts)