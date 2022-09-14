from polymanager.schemas.manticore.manticoresearch_schema import ManticoreSearchSchema
import json
import pytest

class TestManticoreSearchSchema():
    def test_invalid_type(self):
        with pytest.raises(Exception):
            ManticoreSearchSchema("test", "test", {"a": {"type": "double"}})
        
    def test_schema(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}})
        assert  {'fields': {'a': {'type': 'text'}, "id": {"type": "int"}}, 'collection': 'test', 'namespace': 'test'} == manticore_schema.get_schema()

    def test_namespace_fields(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}})
        assert manticore_schema.get_fields() == {'test_a': {'type': 'text'}, "id": {"type": "int"}}

    def test_collection_name(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}})
        assert manticore_schema.get_collection_name() == "test_test"

    def test_collection_name2(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}})
        assert manticore_schema.get_collection_name() == "test_test"

    def test_document_schema(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}})
        with pytest.raises(Exception):
            manticore_schema.check_document_schema({"z": ""})

    def test_document_schema2(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "text"},"b": {"type":"json"}, "id": {"type": "int"}})
        with pytest.raises(Exception):
            manticore_schema.check_document_schema({"a": 9})

    def test_document_schema3(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "int"},"b": {"type":"json"}, "id": {"type": "int"}})
        with pytest.raises(Exception):
            manticore_schema.check_document_schema({"b": 85.3})

    def test_document_schema4(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "json"}, "id": {"type": "int"}})
        with pytest.raises(Exception):
            manticore_schema.check_document_schema({})

    def test_valid_document_schema(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "json"}, "id": {"type": "int"}})
        assert manticore_schema.check_document_schema({"a": json.dumps({"a": "test"}), "id": 1}) == None

    def test_get_document_schema(self):
        manticore_schema = ManticoreSearchSchema("test", "test", {"a": {"type": "timestamp"},"b": {"type":"json"}, "id": {"type": "int"}})
        assert manticore_schema.get_document_schema({"a": "2021/01/01"}) == manticore_schema.get_document_schema({"a": "2021/01/01"})

    def test_valid_manticore_schema(self):
        fields = {
            "a":{
                "type": "text",
                "index": {
                    "stored": False
                }
            },
            "b":{
                "type":"json",
                "index": {

                }
            },
            "c":{
                "type":"timestamp",
                "index": {
                    
                }
            }, "id": {"type": "int"}
        }
        manticore_schema = ManticoreSearchSchema("test", "test", fields, {"min_infix_len": 3})
        req = manticore_schema.generate_schema()
        assert req == '''mode=raw&query=CREATE TABLE test_test (test_a text indexed,test_b json,test_c timestamp) min_infix_len = \'3\''''