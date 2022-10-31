from fastapi import FastAPI
import pytest
from fastapi.testclient import TestClient
from polymanager.containers import CoreContainer, ArangoDBContainer, RedisContainer
from polymanager.helper.conf_helper import load_env
from polymanager.routers import arangodb
import json
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


app = FastAPI()
app.include_router(arangodb.router)

client = TestClient(app)

def test_add_collection(wait_for_databases, clean_databases):
    
    response = client.post("/collection/arangodb", json={
	"collection": "collection1",
	"namespace": "test",
    "global_options": {
        "indexes":[
            {
                "index_type": "fulltext",
                "name": "test_index",
                "fields": ["test"]
            }
        ],
        "edge_collection": False
    },
	"fields":
		{
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        }
    })
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_delete_collection(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
	"collection": "collection2",
	"namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
	"fields":
		{
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        }
    })
    response = client.delete("/collection/arangodb", json={
	"collection": "collection2",
    "empty": True,
	"namespace": "test"
})
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_add_node(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection3",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            },
            "field2": {
                "type": "text"
            }
        }
    })
    response = client.post("/collection/arangodb/node", json={
    "collection": "collection3",
    "namespace": "test",
    "node":
        {
            "field1": "test",
            "field2": 'test2'
        }
    })
    assert json.loads(response.text)["node_id"] != ""
    assert response.status_code == 200

def test_add_arangodb_nodes(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection4",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            },
            "field2": {
                "type": "text"
            }
        }
    })
    response = client.post("/collection/arangodb/nodes", json={
    "collection": "collection4",
    "namespace": "test",
    "nodes":[
        {
            "field1": "test",
            "field2": 'test2'
        },
        {
            "field1": "test",
            "field2": 'test2'
        }
    ]
    })
    assert len(json.loads(response.text)["nodes_id"]) == 2
    assert response.status_code == 200

def test_delete_node(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection5",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            },
            "field2": {
                "type": "text"
            }
        }
    })
    response1 = client.post("/collection/arangodb/node", json={
    "collection": "collection5",
    "namespace": "test",
    "node":
        {
            "field1": "test",
            "field2": 'test2',
        }
    })
    response2 = client.delete("/collection/arangodb/node", json={
    "collection": "collection5",
    "namespace": "test",
    "node_id": json.loads(response1.text)["node_id"]["_key"]
    })
    assert response2.status_code == 200

def test_delete_nodes(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection6",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            },
            "field2": {
                "type": "text"
            }
        }
    })
    response1 = client.post("/collection/arangodb/nodes", json={
    "collection": "collection6",
    "namespace": "test",
    "nodes":[
        {
            "field1": "test",
            "field2": 'test2'
        },
        {
            "field1": "test",
            "field2": 'test2'
        }
    ]
    })
    nodes_id = json.loads(response1.text)["nodes_id"]
    _keys = [nodes_id[0]["_key"], nodes_id[1]["_key"]]
    response2 = client.delete("/collection/arangodb/nodes", json={
    "collection": "collection6",
    "namespace": "test",
    "nodes_id": _keys
    })
    assert response2.status_code == 200

def test_update_node(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection7",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            },
            "field2": {
                "type": "text"
            }
        }
    })
    response1 = client.post("/collection/arangodb/node", json={
    "collection": "collection7",
    "namespace": "test",
    "node":
        {
            "field1": "test",
            "field2": 'test2',
        }
    })
    response2 = client.put("/collection/arangodb/node", json={
    "collection": "collection7",
    "namespace": "test",
    "node":
        {
            "field1": "updated",
            "field2": "updated",
        },
    "node_id": json.loads(response1.text)["node_id"]["_key"]
    })
    arangodb_query = ArangoDBContainer().handler()
    exists = arangodb_query.query("test","""
    FOR d IN `{}`
            FILTER d._key == '{}'
            RETURN d
    """.format("collection7", json.loads(response1.text)["node_id"]["_key"]))
    assert exists["result"][0]["field1"] == "updated"
    assert response2.status_code == 200

def test_update_relationship(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection8",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            }
        }
    })
    response = client.post("/collection/arangodb", json={
    "collection": "collection9",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": True
    },
    "fields":
        {
            "from": {
                "type": "relationship"
            },
            "to": {
                "type": "relationship"
            },
            "label": {
                "type": "text"
            }
        }
    })
    response1 = client.post("/collection/arangodb/nodes", json={
    "collection": "collection8",
    "namespace": "test",
    "nodes":[
        {
            "field1": "test"
        },
        {
            "field1": "test2"
        }
    ]
    })
    nodes_id = json.loads(response1.text)["nodes_id"]
    response2 = client.put("/collection/arangodb/relationship", json={
    "collection": "collection9",
    "namespace": "test",
    "edge": {
            "from": nodes_id[0]["_id"],
            "to": nodes_id[1]["_id"],
            "label": "label"
        }
    })
    arangodb_query = ArangoDBContainer().handler()
    exists = arangodb_query.query("test", """
    WITH collection8
    FOR v, e, p
    IN 1..1
    OUTBOUND '{}'
    collection9
    RETURN p
    """.format(nodes_id[0]["_id"]))
    assert exists["result"][0]["vertices"][1]["field1"] == "test2"
    assert response2.status_code == 200

def test_delete_relationships(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
    "collection": "collection10",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": False
    },
    "fields":
        {
            "field1": {
                "type": "text"
            }
        }
    })
    response = client.post("/collection/arangodb", json={
    "collection": "collection11",
    "namespace": "test",
    "global_options": {
        "indexes":[],
        "edge_collection": True
    },
    "fields":
        {
            "from": {
                "type": "relationship"
            },
            "to": {
                "type": "relationship"
            },
            "label": {
                "type": "text"
            }
        }
    })
    response1 = client.post("/collection/arangodb/nodes", json={
    "collection": "collection10",
    "namespace": "test",
    "nodes":[
        {
            "field1": "test"
        },
        {
            "field1": "test2"
        },
        {
            "field1": "test3"
        }
    ]
    })
    nodes_id = json.loads(response1.text)["nodes_id"]
    response2 = client.put("/collection/arangodb/relationship", json={
    "collection": "collection11",
    "namespace": "test",
    "edge": {
            "from": nodes_id[0]["_id"],
            "to": nodes_id[1]["_id"],
            "label": "label"
        }
    })
    response3 = client.put("/collection/arangodb/relationship", json={
    "collection": "collection11",
    "namespace": "test",
    "edge": {
            "from": nodes_id[1]["_id"],
            "to": nodes_id[2]["_id"],
            "label": "label"
        }
    })
    arangodb_query = ArangoDBContainer().handler()
    exists = arangodb_query.query("test", """
    WITH collection10
    FOR v, e, p
    IN 1..1
    ANY '{}'
    collection11
    RETURN p
    """.format(nodes_id[1]["_id"]))

    edge_id1 = json.loads(response2.text)["edge_id"]["_key"]
    edge_id2 = json.loads(response3.text)["edge_id"]["_key"]
    response4 = client.delete("/collection/arangodb/relationships", json={
    "collection": "collection11",
    "namespace": "test",
    "edges_id": [edge_id1, edge_id2]
    })
    exists = arangodb_query.query("test", """
    WITH collection10
    FOR v, e, p
    IN 1..1
    ANY '{}'
    collection11
    RETURN p
    """.format(nodes_id[1]["_id"]))
    assert len(exists["result"]) == 0

def test_get_collections(wait_for_databases, clean_databases):
    response = client.post("/collection/arangodb", json={
	"collection": "collection9",
	"namespace": "test",
	"fields":
		{
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text"
            }
        }
    })
    response2 = client.get("/collection/arangodb")
    print(response2.text)
    assert response2.text == '{"schemas":[{"fields":{"test":{"type":"text"},"test2":{"type":"text"}},"collection":"collection9","namespace":"test"}],"status":"success"}'