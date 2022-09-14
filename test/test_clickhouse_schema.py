from polymanager.schemas.clickhouse.clickhouse_schema import ClickhouseSchema
import pytest

class TestClickhouseSchema():
    def test_invalid_type(self):
        with pytest.raises(Exception):
            ClickhouseSchema("test", "test", {"a": {"type": "double"}})

    def test_schema(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        assert  {
            'global_collection_opts': {'order_by': ['id']},
            'fields': {'a': {'type': 'text'}, "id": {"type": "int"}},
            'collection': 'test',
            'namespace': 'test'
        } == clickhouse_schema.get_schema()

    def test_collection_name(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        assert "test.test" == clickhouse_schema.get_collection_name()

    def test_document_schema(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "text"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        with pytest.raises(Exception):
            clickhouse_schema.check_document_schema({"z": ""})

    def test_document_schema2(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "text"},"b": {"type":"float"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        with pytest.raises(Exception):
            clickhouse_schema.check_document_schema({"a": 9})

    def test_document_schema3(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "[text]"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        with pytest.raises(Exception):
            clickhouse_schema.check_document_schema({"a": 9})

    def test_document_schema4(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "[text]"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        with pytest.raises(Exception):
            clickhouse_schema.check_document_schema({})

    def test_valid_document_schema(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "[text]"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        assert clickhouse_schema.check_document_schema({"id": 1, "a": [""]}) == None

    def test_get_document_schema(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "timestamp"},"b": {"type":"float"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        assert clickhouse_schema.get_document_schema({"a": "2021/01/01"}) == {'a': '2021-01-01 00:00:00'}

    def test_get_document_schema2(self):
        clickhouse_schema = ClickhouseSchema("test", "test", {"a": {"type": "[text]"}, "id": {"type": "int"}}, global_collection_opts={"order_by":["id"]})
        assert clickhouse_schema.get_document_schema({"a": ["a", "b", "c"]}) == {'a': ["a", "b", "c"]}

    def test_valid_clickhouse_schema(self):
        fields = {
            "a":{
                "type": "text"
            },
            "b":{
                "type":"float"
            },
            "c":{
                "type":"timestamp"
            },
            "d":{
                "type":"[int]"
            },
            "id": {"type": "int"}
        }
        clickhouse_schema = ClickhouseSchema("test", "test", fields, global_collection_opts={"order_by":["id"]})
        req = clickhouse_schema.generate_schema()
        assert req == "query=CREATE TABLE test.test (a String,b Float32,c Datetime,d Array(Int64),id Int64) ENGINE = MergeTree() ORDER BY (id)"