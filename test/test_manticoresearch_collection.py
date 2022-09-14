from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.manticore.manticoresearch_schema import ManticoreSearchSchema
from polymanager.schemas.manticore.manticore_collection import ManticoreSearchCollection
from polymanager.containers import CoreContainer, RedisContainer, ManticoreContainer
from polymanager.exceptions.schema_exception import InvalidSchema
from polymanager.helper.conf_helper import load_env
import pytest

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
    
@pytest.fixture
def clean_databases():
    manticore_handler = ManticoreContainer().handler()
    manticore_handler.query("DROP TABLE IF EXISTS test_collection")
    db_ = RedisContainer.db()
    db_.delete("manticoresearch")

def test_add_document_to_invalid_collection(wait_for_databases, clean_databases):
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    with pytest.raises(Exception):
        ManticoreSearchCollection(test_schema.get_collection_name()+"#error")

def test_add_document_with_invalid_schema(wait_for_databases, clean_databases):
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    with pytest.raises(InvalidSchema):
        doc_collection.add_document({"test": "test", "test2": 1})

def test_add_document(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    res = doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x1})
    res = manticore_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert res["data"][0]["count(*)"]== 1

def test_truncate(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    res = doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x1})
    doc_collection.truncate()
    res = manticore_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert res["data"][0]["count(*)"] == 0

def test_add_documents(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "stored": True
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    res = doc_collection.add_documents([{"test": "test", "test2": "test2", "id": 0x1}, {"test": "test", "test2": "test2", "id": 0x2}])
    res = manticore_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    res3 = manticore_handler.query("select test_test2 from {}".format(test_schema.get_collection_name()))
    assert res["data"][0]["count(*)"] == 2
    assert "test_test2" in res3["data"][0]

def test_delete_document(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x1})
    doc_collection.delete_document(0x1)
    res = manticore_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert res["data"][0]["count(*)"] == 0

def test_delete_documents(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x1})
    doc_collection.add_document({"test": "test", "test2": "test2", "id": 0x2})
    doc_collection.delete_documents([0x1, 0x2])
    res = manticore_handler.query("select count(*) from {}".format(test_schema.get_collection_name()))
    assert res["data"][0]["count(*)"] == 0

def test_update_document(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    test_schema = ManticoreSearchSchema("test","collection", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                }
            },
            "test3": {
                "type": "timestamp"
            },
            "id": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema( "manticoresearch")
    internal_schema.save_schema(test_schema)
    doc_collection = ManticoreSearchCollection(internal_schema, test_schema.get_collection_name())
    doc_collection.add_document({"test": "test", "test2": "test2", "test3":"2021-01-01 00:00", "id": 0x999})
    doc_collection.update_document({"test": "updated", "test2": "updated", "test3":"2000-01-01 00:00", "id": 0x999})
    res = manticore_handler.query("select test_test3 test3 from {} where match('updated');".format(test_schema.get_collection_name()))
    assert res["data"][0]["test3"] == 946681200