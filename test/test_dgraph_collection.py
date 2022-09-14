from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.dgraph.dgraph_schema import DGraphSchema
from polymanager.schemas.dgraph.dgraph_collection import DgraphCollection
from polymanager.containers import CoreContainer, DGraphContainer, RedisContainer
from polymanager.exceptions.schema_exception import InvalidSchema, UnkownSchema
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
    dgraph_handler = DGraphContainer().handler()
    ready = False
    while not ready:
        try:
            res = dgraph_handler.get_health()
            if res and res[0]["status"] == "healthy":
                ready = True
        except Exception:
            pass
    
@pytest.fixture
def clean_databases():
    dgraph_handler = DGraphContainer().handler()
    res = dgraph_handler.drop_all()
    db_ = RedisContainer.db()
    db_.delete("dgraph")
    

def test_add_node_to_invalid_collection(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection1", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    with pytest.raises(UnkownSchema):
        DgraphCollection(internal_schema, test_schema.get_collection_name()+"#error")

def test_add_node_with_invalid_schema(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection2", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    with pytest.raises(InvalidSchema):
        node_collection.add_node({"test": "test", "test2": 1})

def test_add_node(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection3", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    res = node_collection.add_node({"test": "test", "test2": "test2"})
    assert res["node_id"] is not None
    
def test_truncate(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection4", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    res = node_collection.add_node({"test": "test", "test2": "test2"})
    node_collection.truncate()
    dgraph_query = DGraphContainer().handler()
    exists = dgraph_query.query("""{{
    query(func: type({})){{
        count(uid)
    }}
}}""".format(test_schema.get_collection_name()))
    assert exists["query"][0]["count"] == 0

def test_add_nodes(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection5", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    res = node_collection.add_nodes([{"test": "test", "test2": "test2"}, {"test": "test", "test2": "test2"}])
    assert len(res["nodes_id"]) == 2

def test_delete_node(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection6", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    node_id = node_collection.add_node({"test": "test", "test2": "test2"})
    dgraph_handler = DGraphContainer().handler()
    status = dgraph_handler.node_exists(test_schema.get_namespace(), test_schema.get_collection_name(), node_id["node_id"])
    assert status is not None
    node_collection.delete_node(node_id["node_id"])
    status = dgraph_handler.node_exists(test_schema.get_namespace(), test_schema.get_collection_name(), node_id["node_id"])
    assert status is None

def test_delete_nodes(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection7", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    nodes_id = node_collection.add_nodes([{"test": "test", "test2": "test2"}, {"test": "test", "test2": "test2"}])
    dgraph_handler = DGraphContainer().handler()
    node_collection.delete_nodes(nodes_id["nodes_id"])
    deleted = True
    for node in nodes_id["nodes_id"]:
        status = dgraph_handler.node_exists(test_schema.get_namespace(), test_schema.get_collection_name(), node)
        if status:
            deleted = False
    assert deleted == True

def test_update_node(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection8", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    node_id = node_collection.add_node({"test": "test", "test2": "test2"})
    dgraph_handler = DGraphContainer().handler()
    pred = dgraph_handler.get_predicate(node_id["node_id"], "polymanager.test.test")
    assert pred["polymanager.test.test"] == "test"
    node_collection.update_node(node_id["node_id"], {"test": "updated", "test2": "updated"})
    pred = dgraph_handler.get_predicate(node_id["node_id"], "polymanager.test.test")
    assert pred["polymanager.test.test"] == "updated"

def test_update_relationship(wait_for_databases, clean_databases):
    test_schema = DGraphSchema("polymanager.test","collection9", {
            "attr_link": {
                "type": "relationship"
            },
            "attr_str": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("dgraph")
    internal_schema.save_schema(test_schema)
    node_collection = DgraphCollection(internal_schema, test_schema.get_collection_name())
    node_id = node_collection.add_node({"attr_str": "test"})
    node_id2 = node_collection.add_node({"attr_str": "test2"})
    dgraph_handler = DGraphContainer().handler()
    node_collection.update_relationship(node_id["node_id"], {"attr_link": [node_id2["node_id"]]})
    pred2 = dgraph_handler.get_predicate_edges(node_id["node_id"], "polymanager.test.attr_link")
    assert len(pred2[0]["polymanager.test.attr_link"]) == 1