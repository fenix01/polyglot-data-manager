from fastapi import FastAPI
import pytest
from fastapi.testclient import TestClient
from polymanager.containers import CoreContainer, RedisContainer, ClickhouseContainer
from polymanager.helper.conf_helper import load_env
from polymanager.routers import clickhouse

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

# 
app = FastAPI()
app.include_router(clickhouse.router)
client = TestClient(app)


def test_get_collections(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection1",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response2 = client.get("/collection/clickhouse")
    assert response2.text == '{"schemas":[{"fields":{"test_field1":{"type":"text"},"test_field2":{"type":"text"},"id":{"type":"int"}},"collection":"collection1","namespace":"test","global_collection_opts":{"order_by":["id"]}}],"status":"success"}'

def test_add_collection(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection1",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_delete_collection(wait_for_databases, clean_databases):
    response = client.post("/collection/clickhouse", json={
	"collection": "collection2",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.delete("/collection/clickhouse", json={
	"collection": "collection2",
    "empty": True,
	"namespace": "test"
})
    assert response.status_code == 200
    #assert response.json() == {'detail': {'error': 'this collection already exists', 'status': 'failed'}}

def test_add_document(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection3",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.post("/collection/clickhouse/document", json={
    "collection": "collection3",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        }
    })
    res = clickhouse_handler.query("select count(*) from {}".format("test.collection3"))
    assert int(res.text.splitlines()[0]) == 1

def test_add_documents(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection4",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.post("/collection/clickhouse/documents", json={
        "collection": "collection4",
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
    res = clickhouse_handler.query("select count(*) from {}".format("test.collection4"))
    assert int(res.text.splitlines()[0]) == 2

def test_add_empty_documents(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection5",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.post("/collection/clickhouse/documents", json={
        "collection": "collection5",
        "namespace": "test",
        "documents":[
        ]
    },
        )
    res = clickhouse_handler.query("select count(*) from {}".format("test.collection5"))
    assert response.status_code == 422

def test_delete_document(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection6",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.post("/collection/clickhouse/document", json={
    "collection": "collection6",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 1
        }
    })
    response = client.delete("/collection/clickhouse/document", json={
    "collection": "collection6",
    "namespace": "test",
    "document_id": 1
    })
    res = clickhouse_handler.query("select count(*) from {}".format("test.collection6"))
    assert int(res.text.splitlines()[0]) == 0

def test_delete_documents(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection7",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.post("/collection/clickhouse/documents", json={
        "collection": "collection7",
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
    response = client.delete("/collection/clickhouse/documents", json={
    "collection": "collection7",
    "namespace": "test",
    "documents_id": [1,2]
    })
    res = clickhouse_handler.query("select count(*) from {}".format("test.collection7"))
    assert int(res.text.splitlines()[0]) == 0

def test_update_document(wait_for_databases, clean_databases):
    clickhouse_handler = ClickhouseContainer().handler()
    response = client.post("/collection/clickhouse", json={
	"collection": "collection8",
	"namespace": "test",
    "global_options": {"order_by":["id"]},
	"fields":
		{
            "test_field1": {
                "type": "text"
            },
            "test_field2": {
                "type":"text"
            },
            "id": {
                "type":"int"
            }
	    }
    })
    response = client.post("/collection/clickhouse/document", json={
    "collection": "collection8",
    "namespace": "test",
    "document":
        {
            "test_field1": "test",
            "test_field2": '{"test":"test"}',
            "id": 0x999
        }
    })
    response = client.put("/collection/clickhouse/document", json={
    "collection": "collection8",
    "namespace": "test",
    "document": {
        "test_field1": "updated",
        "test_field2": '{"updated":"updated"}',
        "id": 0x999
    }
    })
    res = clickhouse_handler.query("select * from {}".format("test.collection8"))
    assert res.text.splitlines()[0].split("\t")[0] == "updated"