from fastapi import FastAPI
import pytest
from fastapi.testclient import TestClient
from polymanager.containers import CoreContainer, DGraphContainer, RedisContainer
from polymanager.helper.conf_helper import load_env
from polymanager.routers import dgraph
import time
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


app = FastAPI()
app.include_router(dgraph.router)

client = TestClient(app)

def test_add_collection(wait_for_databases, clean_databases):
    
    response = client.post("/collection/dgraph", json={
	"collection": "collection1",
	"namespace": "polymanager.test",
	"fields":
		{
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_delete_collection(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
	"collection": "collection2",
	"namespace": "polymanager.test",
	"fields":
		{
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response = client.delete("/collection/dgraph", json={
	"collection": "collection2",
    "empty": True,
	"namespace": "polymanager.test"
})
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_add_node(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
    "collection": "collection3",
    "namespace": "polymanager.test",
    "fields":
        {
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response = client.post("/collection/dgraph/node", json={
    "collection": "collection3",
    "namespace": "polymanager.test",
    "node":
        {
            "test_field1": "test",
            "test_field2": 'test2'
        }
    })
    assert json.loads(response.text)["node_id"] != ""
    assert response.status_code == 200

def test_add_dgraph_nodes(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
    "collection": "collection4",
    "namespace": "polymanager.test",
    "fields":
        {
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response = client.post("/collection/dgraph/nodes", json={
    "collection": "collection4",
    "namespace": "polymanager.test",
    "nodes":[
        {
            "test_field1": "test",
            "test_field2": 'test2'
        },
        {
            "test_field1": "test",
            "test_field2": 'test2'
        }
    ]
    })
    assert len(json.loads(response.text)["nodes_id"]) == 2
    assert response.status_code == 200

def test_delete_node(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
    "collection": "collection5",
    "namespace": "polymanager.test",
    "fields":
        {
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response1 = client.post("/collection/dgraph/node", json={
    "collection": "collection5",
    "namespace": "polymanager.test",
    "node":
        {
            "test_field1": "test",
            "test_field2": 'test2',
        }
    })
    response2 = client.delete("/collection/dgraph/node", json={
    "collection": "collection5",
    "namespace": "polymanager.test",
    "node_id": json.loads(response1.text)["node_id"]
    })
    assert response2.status_code == 200

def test_delete_nodes(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
    "collection": "collection6",
    "namespace": "polymanager.test",
    "fields":
        {
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response1 = client.post("/collection/dgraph/nodes", json={
    "collection": "collection6",
    "namespace": "polymanager.test",
    "nodes":[
        {
            "test_field1": "test",
            "test_field2": 'test2'
        },
        {
            "test_field1": "test",
            "test_field2": 'test2'
        }
    ]
    })
    response2 = client.delete("/collection/dgraph/nodes", json={
    "collection": "collection6",
    "namespace": "polymanager.test",
    "nodes_id": json.loads(response1.text)["nodes_id"]
    })
    assert response2.status_code == 200

def test_update_node(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
    "collection": "collection7",
    "namespace": "polymanager.test",
    "fields":
        {
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response1 = client.post("/collection/dgraph/node", json={
    "collection": "collection7",
    "namespace": "polymanager.test",
    "node":
        {
            "test_field1": "test",
            "test_field2": 'test2',
        }
    })
    response2 = client.put("/collection/dgraph/node", json={
    "collection": "collection7",
    "namespace": "polymanager.test",
    "node":
        {
            "test_field1": "updated",
            "test_field2": "updated",
        },
    "node_id": json.loads(response1.text)["node_id"]
    })
    dgraph_query = DGraphContainer().handler()
    exists = dgraph_query.query("""{{
  query(func: uid({})){{
    uid
    polymanager.test.test_field1
    polymanager.test.test_field2
  }}
}}""".format(json.loads(response1.text)["node_id"]))
    assert exists["query"][0]["polymanager.test.test_field1"] == "updated"
    assert response2.status_code == 200

def test_update_relationship(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
    "collection": "collection8",
    "namespace": "polymanager.test",
    "fields":
        {
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type": "relationship"
            }
        }
    })
    response1 = client.post("/collection/dgraph/nodes", json={
    "collection": "collection8",
    "namespace": "polymanager.test",
    "nodes":[
        {
            "test_field1": "test"
        },
        {
            "test_field1": "test"
        }
    ]
    })
    response2 = client.put("/collection/dgraph/relationship", json={
    "collection": "collection8",
    "namespace": "polymanager.test",
    "node":
        {
            "test_field2": [json.loads(response1.text)["nodes_id"][1]],
        },
    "node_id": json.loads(response1.text)["nodes_id"][0]
    })
    dgraph_query = DGraphContainer().handler()
    exists = dgraph_query.query("""{{
  query(func: uid({})){{
    uid
    polymanager.test.test_field2 {{
        uid
    }}
  }}
}}""".format(json.loads(response1.text)["nodes_id"][0]))
    assert exists["query"][0]["polymanager.test.test_field2"][0]["uid"] == json.loads(response1.text)["nodes_id"][1]
    assert response2.status_code == 200

def test_get_collections(wait_for_databases, clean_databases):
    response = client.post("/collection/dgraph", json={
	"collection": "collection9",
	"namespace": "polymanager.test",
	"fields":
		{
            "test": {
                "type": "text"
            },
            "test2": {
                "type": "text",
                "index":{
                    "tokenizer": "hash"
                }
            }
        }
    })
    response2 = client.get("/collection/dgraph")
    assert response2.text == '{"schemas":[{"fields":{"test":{"type":"text"},"test2":{"type":"text","index":{"tokenizer":"hash"}}},"collection":"collection9","namespace":"polymanager.test"}],"status":"success"}'