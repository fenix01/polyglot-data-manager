from fastapi import FastAPI
import pytest
from fastapi.testclient import TestClient
from polymanager.containers import CoreContainer, RedisContainer, ManticoreContainer
from polymanager.helper.conf_helper import load_env
from polymanager.routers import manticoresearch

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

app = FastAPI()
app.include_router(manticoresearch.router)


client = TestClient(app)

def test_get_collections(wait_for_databases, clean_databases):
    response = client.post("/collection/manticoresearch", json={
	"collection": "collection",
	"namespace": "test",
	"fields":
		{
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response2 = client.get("/collection/manticoresearch")
    assert response2.text == '{"schemas":[{"fields":{"test_field1":{"type":"text","index":{}},"test_field2":{"type":"json"},"id":{"type":"int"}},"collection":"collection","namespace":"test"}],"status":"success"}'

def test_add_collection(wait_for_databases, clean_databases):
    
    response = client.post("/collection/manticoresearch", json={
	"collection": "collection",
	"namespace": "test",
	"fields":
		{
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_delete_collection(wait_for_databases, clean_databases):
    response = client.post("/collection/manticoresearch", json={
	"collection": "collection",
	"namespace": "test",
	"fields":
		{
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.delete("/collection/manticoresearch", json={
	"collection": "collection",
    "empty": False,
	"namespace": "test"
})
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_add_document(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    response = client.post("/collection/manticoresearch", json={
    "collection": "collection",
    "namespace": "test",
    "fields":
        {
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
        }
    })
    response = client.post("/collection/manticoresearch/document", json={
    "collection": "collection",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        }
    })
    res = manticore_handler.query("select count(*) from {}".format("test_collection"))
    assert res["data"][0]["count(*)"] == 1


def test_options_collection(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    response = client.post("/collection/manticoresearch", json={
    "collection": "collection",
    "namespace": "test",
    "global_options":{
        "min_infix_len": 3
    },
    "fields":
        {
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
        }
    })
    response = client.post("/collection/manticoresearch/document", json={
    "collection": "collection",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        }
    })
    res = manticore_handler.query("select * from {} where match('te*')".format("test_collection"))
    assert res["data"][0]["id"] == 1

def test_add_documents(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    response = client.post("/collection/manticoresearch", json={
    "collection": "collection",
    "namespace": "test",
    "fields":
        {
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
        }
    })
    response = client.post("/collection/manticoresearch/documents", json={
        "collection": "collection",
        "namespace": "test",
        "documents":[
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        },
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 2
        }
        ]
    },
        )
    res = manticore_handler.query("select count(*) from {}".format("test_collection"))
    assert res["data"][0]["count(*)"] == 2

def test_delete_document(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    response = client.post("/collection/manticoresearch", json={
    "collection": "collection",
    "namespace": "test",
    "fields":
        {
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
        }
    })
    response = client.post("/collection/manticoresearch/document", json={
    "collection": "collection",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        }
    })
    response = client.delete("/collection/manticoresearch/document", json={
    "collection": "collection",
    "namespace": "test",
    "document_id": 1
    })
    res = manticore_handler.query("select count(*) from {}".format("test_collection"))
    assert res["data"][0]["count(*)"] == 0

def test_delete_documents(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    response = client.post("/collection/manticoresearch", json={
    "collection": "collection",
    "namespace": "test",
    "fields":
        {
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
        }
    })
    response = client.post("/collection/manticoresearch/documents", json={
        "collection": "collection",
        "namespace": "test",
        "documents":[
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        },
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 2
        }
        ]
    },
        )
    response = client.delete("/collection/manticoresearch/documents", json={
    "collection": "collection",
    "namespace": "test",
    "documents_id": [1,2]
    })
    res = manticore_handler.query("select count(*) from {}".format("test_collection"))
    assert res["data"][0]["count(*)"] == 0

def test_update_document(wait_for_databases, clean_databases):
    manticore_handler = ManticoreContainer().handler()
    response = client.post("/collection/manticoresearch", json={
    "collection": "collection",
    "namespace": "test",
    "fields":
        {
            "test_field1": {
                "type": "text",
                "index": {
                }
            },
            "test_field2": {
                "type":"json"
            },
            "id": {
                "type":"int"
            }
        }
    })
    response = client.post("/collection/manticoresearch/document", json={
    "collection": "collection",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 0x999
        }
    })
    response = client.put("/collection/manticoresearch/document", json={
    "collection": "collection",
    "namespace": "test",
    "document": {
        "test_field1": "updated",
        "test_field2": '{"updated":"updated"}',
        "id": 0x999
    }
    })
    res = manticore_handler.query("select * from {} where match('updated');".format("test_collection"))
    assert res["data"][0]["id"] == 0x999