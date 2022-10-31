from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.arangodb.arangodb_schema import ArangodbSchema
from polymanager.schemas.arangodb.arangodb_collection import ArangodbCollection
from polymanager.containers import CoreContainer, ArangoDBContainer, RedisContainer
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
    arangodb_handler = ArangoDBContainer().handler()
    ready = False
    while not ready:
        try:
            res = arangodb_handler.get_health()
            if res and res["code"] == 200:
                ready = True
        except Exception as e:
            pass
    
@pytest.fixture
def clean_databases():
    arangodb_handler = ArangoDBContainer().handler()
    res = arangodb_handler.drop_all()
    db_ = RedisContainer.db()
    db_.delete("arangodb")
    

def test_add_node_to_invalid_collection(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection1", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    with pytest.raises(UnkownSchema):
        ArangodbCollection(internal_schema, test_schema.get_collection_name()+"#error")

def test_add_node_with_invalid_schema(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection2", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    with pytest.raises(InvalidSchema):
        node_collection.add_node({"test": "test", "test2": 1})

def test_add_node_with_invalid_schema2(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection2", {
            "test": {
                "type": "[text]"
            },
            "test2": {
                "type": "json"
            },
            "test3": {
                "type": "int"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    with pytest.raises(InvalidSchema):
        node_collection.add_node({"test": ["test"], 'test2':"{'test2':'test2'}", 'test3': '1'})

def test_add_node(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection2", {
            "test": {
                "type": "[text]"
            },
            "test2": {
                "type": "json"
            },
            "test3": {
                "type": "int"
            },
            "test4": {
                "type": "timestamp"
            }
        },
        global_collection_opts = {
            "indexes": [
                {
                    "index_type": "fulltext",
                    "name": "test2_index",
                    "fields": ["test2"]
                },
                {
                    "index_type": "hash",
                    "name": "test3_index",
                    "fields": ["test3"]
                },
                {
                    "index_type": "persistent",
                    "name": "test4_index",
                    "fields": ["test4"]
                }
            ],
            "edge_collection": False
        })
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    res = node_collection.add_node({"test": ["test"], 'test2':'{"test2":"test2"}', 'test3': 1, "test4": "2022-08-30T07:15:06Z"})
    assert res["node_id"] is not None
    
def test_truncate(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection4", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    res = node_collection.add_node({"test": "test", "test2": "test2"})
    node_collection.truncate()
    arangodb_query = ArangoDBContainer().handler()
    exists = arangodb_query.query(
        test_schema.get_namespace(),
        """RETURN LENGTH({})""".format(test_schema.get_collection_name()))
    assert exists["result"][0] == 0

def test_add_nodes(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection5", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    res = node_collection.add_nodes([{"test": "test", "test2": "test2"}, {"test": "test", "test2": "test2"}])
    assert len(res["nodes_id"]) == 2

def test_delete_node(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection6", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    node = node_collection.add_node({"test": "test", "test2": "test2"})
    arangodb_handler = ArangoDBContainer().handler()
    status = arangodb_handler.node_exists(
        test_schema.get_namespace(),
        test_schema.get_collection_name(),
        node["node_id"]["_key"]
        )
    assert status["result"][0] is not None
    node_collection.delete_node(node["node_id"]["_key"])
    status = arangodb_handler.node_exists(
        test_schema.get_namespace(),
        test_schema.get_collection_name(),node["node_id"]["_key"]
    )
    assert status["result"][0] is None

def test_delete_nodes(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection7", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    nodes = node_collection.add_nodes([{"test": "test", "test2": "test2"}, {"test": "test", "test2": "test2"}])
    nodes_id = [nodes["nodes_id"][0]["_key"], nodes["nodes_id"][1]["_key"]]
    arangodb_handler = ArangoDBContainer().handler()
    node_collection.delete_nodes(nodes_id)
    deleted = True
    for node_id in nodes_id:
        status = arangodb_handler.node_exists(
            test_schema.get_namespace(),
            test_schema.get_collection_name(),
            node_id)
        if status["result"][0]:
            deleted = False
    assert deleted == True

def test_update_node(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection8", {
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        },
        global_collection_opts = {
            "indexes": [],
            "edge_collection": False
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    node = node_collection.add_node({"test": "test", "test2": "test2"})
    arangodb_handler = ArangoDBContainer().handler()
    pred = arangodb_handler.get_predicate(
        test_schema.get_namespace(),
        test_schema.get_collection_name(),
        node["node_id"]["_key"],
        "test")
    assert pred == "test"
    node_collection.update_node(node["node_id"]["_key"], {"test": "updated", "test2": "updated"})
    pred = arangodb_handler.get_predicate(
        test_schema.get_namespace(),
        test_schema.get_collection_name(),
        node["node_id"]["_key"],
        "test")
    assert pred == "updated"

def test_update_relationship(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection9", {
            "attr1": {
                "type": "text"
            },
            "attr2": {
                "type": "text"
            }
        },
        global_collection_opts = {
            "indexes": [],
            "edge_collection": False
        })
    test_edge_schema = ArangodbSchema("test","collection10", {
            "from": {
                "type": "relationship"
            },
            "to": {
                "type": "relationship"
            },
            "label": {
                "type": "text"
            }
        },
    global_collection_opts = {
            "indexes": [{
                    "index_type": "fulltext",
                    "name": "test_index",
                    "fields": ["label"]
                }],
            "edge_collection": True
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    internal_schema.save_schema(test_edge_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    edge_collection = ArangodbCollection(internal_schema, test_edge_schema.get_collection_name())
    node = node_collection.add_node({"attr1": "test","attr2": "test2"})
    node2 = node_collection.add_node({"attr1": "test2","attr2": "test2"})
    arangodb_handler = ArangoDBContainer().handler()
    edge_collection.update_relationship({"from": node["node_id"]["_id"], "to": node2["node_id"]["_id"], "label": "contains"})
    edge_collection.update_relationship({"from": node["node_id"]["_id"], "to": node2["node_id"]["_id"], "label": "contains2"})
    res = arangodb_handler.get_edges(
        test_edge_schema.get_namespace(),
        test_edge_schema.get_collection_name(),
        node["node_id"]["_id"]
    )
    assert res[0]["label"] == "contains2"

def test_delete_relationships(wait_for_databases, clean_databases):
    test_schema = ArangodbSchema("test","collection11", {
            "attr1": {
                "type": "text"
            },
            "attr2": {
                "type": "text"
            }
        },
        global_collection_opts = {
            "indexes": [],
            "edge_collection": False
        })
    test_edge_schema = ArangodbSchema("test","collection12", {
            "from": {
                "type": "relationship"
            },
            "to": {
                "type": "relationship"
            },
            "label": {
                "type": "text"
            }
        },
    global_collection_opts = {
            "indexes": [{
                    "index_type": "fulltext",
                    "name": "test_index",
                    "fields": ["label"]
                }],
            "edge_collection": True
        })
    
    internal_schema = KVRocksInternalSchema("arangodb")
    internal_schema.save_schema(test_schema)
    internal_schema.save_schema(test_edge_schema)
    node_collection = ArangodbCollection(internal_schema, test_schema.get_collection_name())
    edge_collection = ArangodbCollection(internal_schema, test_edge_schema.get_collection_name())
    node = node_collection.add_node({"attr1": "test1","attr2": "test2"})
    node2 = node_collection.add_node({"attr1": "test1","attr2": "test2"})
    node3 = node_collection.add_node({"attr1": "test1","attr2": "test2"})
    edge1 = edge_collection.update_relationship({"from": node["node_id"]["_id"], "to": node2["node_id"]["_id"], "label": "contains"})
    edge2 = edge_collection.update_relationship({"from": node2["node_id"]["_id"], "to": node3["node_id"]["_id"], "label": "contains"})
    arangodb_handler = ArangoDBContainer().handler()
    res = arangodb_handler.get_edges(
        test_edge_schema.get_namespace(),
        test_edge_schema.get_collection_name(),
        node2["node_id"]["_id"]
    )
    assert len(res) == 2
    edge_collection.delete_relationships([edge1["edge_id"]["_key"], edge2["edge_id"]["_key"]])
    res = arangodb_handler.get_edges(
        test_edge_schema.get_namespace(),
        test_edge_schema.get_collection_name(),
        node2["node_id"]["_id"]
    )
    assert len(res) == 0