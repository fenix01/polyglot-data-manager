from polymanager.schemas.dgraph.dgraph_schema import DGraphSchema
import pytest

class TestDGraphSchema():
    def test_invalid_type(self):
        with pytest.raises(Exception):
            DGraphSchema("test", "test", {"a": {"type": "double"}})

    def test_schema(self):
        dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "text"}})
        assert dgraph_schema.get_schema() == {'fields': {'a': {'type': 'text'}}, 'collection': 'test', 'namespace': 'test'}

    def test_namespace_fields(self):
        dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "text"}})
        assert dgraph_schema.get_fields() == {'test.a': {'type': 'text'}}

    def test_collection_name(self):
        dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "text"}})
        assert dgraph_schema.get_collection_name() == "test.test"

    def test_collection_name2(self):
        dgraph_schema = DGraphSchema( "test", "test", {"a": {"type": "text"}})
        assert dgraph_schema.get_collection_name() == "test.test"

    def test_node_schema(self):
        dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "text"}})
        with pytest.raises(Exception):
            dgraph_schema.check_node_schema({"z": ""})

    def test_get_node_schema(self):
        dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "timestamp"},"b": {"type":"relationship"}})
        assert dgraph_schema.get_node_schema({"a": "2021/01/01"}) == {'test.a': '2021-01-01T00:00:00'}

    def test_relationship_schema(self):
        with pytest.raises(Exception):
            dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "text"},"b": {"type":"relationship"}})
            dgraph_schema.check_relationship_schema({"a": ""})
        
    def test_relationship_fields(self):
        dgraph_schema = DGraphSchema("test", "test", {"a": {"type": "text"},"b": {"type":"relationship"}})
        assert dgraph_schema.get_relationship_fields() == ["test.b"]

    def test_valid_dgraph_schema(self):
        fields = {
            "a": {
                "type": "text"
            },
            "b": {
                "type":"relationship"
            }
        }
        dgraph_schema = DGraphSchema("test", "test", fields)
        req = dgraph_schema.generate_schema()
        assert req == '''test.a: string .
test.b: [uid] .
type <test.test> {
test.a
test.b
}'''

    def test_invalid_tokenizer(self):
        fields = {
            "a": {
                "type": "text",
                "index": {
                    "tokenizer": "none"
                }
            },
            "b": {
                "type":"relationship"
            }
        }
        with pytest.raises(Exception):
            DGraphSchema("test", "test", fields)

    def test_valid_tokenizer(self):
        fields = {
            "a": {
                "type": "text",
                "index": {
                    "tokenizer": "hash"
                }
            },
            "b": {
                "type":"relationship"
            }
        }
        dgraph_schema = DGraphSchema("test", "test", fields)
        req = dgraph_schema.generate_schema()
        assert req == '''test.a: string @index(hash) .
test.b: [uid] .
type <test.test> {
test.a
test.b
}'''