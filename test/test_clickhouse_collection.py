import pytest
from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.clickhouse.clickhouse_schema import ClickhouseSchema
from polymanager.schemas.clickhouse.clickhouse_collection import ClickhouseCollection
from polymanager.containers import CoreContainer, ClickhouseContainer, RedisContainer
from polymanager.exceptions.schema_exception import InvalidSchema
from polymanager.helper.conf_helper import load_env
from redis import ConnectionError
    
pytest_plugins = ["docker_compose"]

@pytest.fixture(scope="session")
def wait_for_databases(session_scoped_container_getter):
    CoreContainer.config.override(load_env())
    db_ = RedisContainer.db()
    ready = False
    while not ready:
        try:
            db_.ping()
            ready = True
        except ConnectionError:
            pass
    clickhouse_handler = ClickhouseContainer().handler()
    ready = False
    while not ready:
        try:
            res = clickhouse_handler.ping()
            if res == "Ok.\n":
                ready = True
        except Exception:
            pass
    
@pytest.fixture
def clean_databases():
    clickhouse_handler = ClickhouseContainer().handler()
    clickhouse_handler.query("DROP DATABASE test")
    db_ = RedisContainer.db()
    db_.delete("clickhouse")

def test_add_document_to_invalid_collection(wait_for_databases, clean_databases):
    test_schema = ClickhouseSchema("test","collection1", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            },
            "id": {
                "type": "int"
            }
        }, global_collection_opts={'order_by': ['id']})

    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    with pytest.raises(Exception):
        ClickhouseCollection(internal_schema, test_schema.get_collection_name()+"#error")

def test_add_document_with_invalid_schema(wait_for_databases, clean_databases):
    test_schema = ClickhouseSchema("test","collection2", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            },
            "id": {
                "type": "int"
            }
        }, global_collection_opts={'order_by': ['id']})

    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    with pytest.raises(InvalidSchema):
        doc_collection.add_document({"test": "test", "test2": 1})

def test_add_document(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    test_schema = ClickhouseSchema("test","collection3", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "int"
            },
            "test3": {
                "type": "timestamp"
            },
            "test4":{
                "type": "[text]"
            },
            "id": {
                "type": "int"
            }
        }, global_collection_opts={'order_by': ['id']})
    
    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    #res = doc_collection.add_document({"test": "test", "test2": 58, "test3": '2021-01-01T00:00:00', "test4":["a", "b"], "id": 0x1})
    res = doc_collection.add_document({"test": "test", "test2": 58, "test3": '2021-05-03T14:56:34Z', "test4":["a", "b"], "id": 0x1})
    res = clickhouse_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    res2 = clickhouse_handler.query("select * from {} where id = 0x1".format(test_schema.get_collection_name()))
    assert int(res.text.splitlines()[0]) == 1
    assert res2.text.split("\t")[3] == "['a','b']"
    assert res2.text.split("\t")[2] == "2021-05-03 14:56:34"

def test_truncate(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    test_schema = ClickhouseSchema("test","collection4", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "int"
            },
            "test3": {
                "type": "timestamp"
            },
            "test4":{
                "type": "[text]"
            },
            "id": {
                "type": "int"
            }
        }, global_collection_opts={'order_by': ['id']})
    
    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    res = doc_collection.add_document({"test": "test", "test2": 58, "test3": '2021-05-03T14:56:34Z', "test4":["a", "b"], "id": 0x1})
    res = doc_collection.truncate()
    res = clickhouse_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert int(res.text.splitlines()[0]) == 0

def test_add_documents(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    test_schema = ClickhouseSchema("test","collection5", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "int"
            },
            "test3": {
                "type": "timestamp"
            },
            "test4":{
                "type": "[text]"
            },
            "id": {
                "type": "int"
            }
        }, global_collection_opts={'order_by': ['id']})
    
    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    res = doc_collection.add_documents([{"test": "test", "test2": 58, "test3": '2021-01-01T00:00:00', "test4":["a", "b"], "id": 0x1}, {"test": "test", "test2": 58, "test3": '2021-01-01T00:00:00', "test4":["a", "b"], "id": 0x2}])
    res = clickhouse_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert int(res.text.splitlines()[0]) == 2

def test_delete_document(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    test_schema = ClickhouseSchema("test","collection6", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            },
            "id": {
                "type": "int"
            }
        }, global_collection_opts={'order_by': ['id']})
    
    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x1})
    doc_collection.delete_document(0x1)
    res = clickhouse_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert int(res.text.splitlines()[0]) == 0

def test_delete_documents(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    test_schema = ClickhouseSchema("test","collection7", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            },
            "id": {
                "type": "int"
            }
	    }, global_collection_opts={'order_by': ['id']})
    
    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x1})
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x2})
    doc_collection.delete_documents([0x1, 0x2])
    res = clickhouse_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert int(res.text.splitlines()[0]) == 0

def test_update_document(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    test_schema = ClickhouseSchema("test","collection8", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            },
            "id": {
                "type": "int"
            }
	    }, global_collection_opts={'order_by': ['id']})
    
    internal_schema = KVRocksInternalSchema("clickhouse")
    internal_schema.save_schema(test_schema)
    doc_collection = ClickhouseCollection(internal_schema, test_schema.get_collection_name())
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x999})
    doc_collection.update_document({"test": "updated", "test2": "updated", "id": 0x999})
    res = clickhouse_handler.query("select * from {} where id=0x999;".format(test_schema.get_collection_name()))
    assert res.text.split("\t")[0] == "updated"