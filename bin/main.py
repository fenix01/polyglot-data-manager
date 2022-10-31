import uvicorn
from fastapi import FastAPI
from polymanager.containers import CoreContainer
from polymanager.routers import dgraph
from polymanager.routers import manticoresearch
from polymanager.routers import clickhouse
from polymanager.routers import arangodb
from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.helper.conf_helper import load_env
from fastapi.routing import APIRoute
from fastapi.openapi.utils import get_openapi

app = FastAPI(title="Polyglot Data Manager")

class PolyglotDataManager:

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title="Polyglot Data Manager",
            version="1.0.0",
            description="The goal of this tool is to help provisioning schemas with only a REST API. You will be able to abstract the creation of schemas, to manage more easily the provisioning of these schemas and to write data by enforcing a schema.",
            routes=app.routes,
            servers=[{"url": "http://127.0.0.1:8016", "description": ""}]
        )
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    def __init__(self):
        CoreContainer.config.override(load_env())

        #start app
        
        app.openapi = PolyglotDataManager.custom_openapi
        #prepare internal schema
        for datastore in CoreContainer.config.connectors():
            if datastore == "manticoresearch":
                internal_schema = KVRocksInternalSchema( "manticoresearch")
                app.include_router(manticoresearch.router)
            elif datastore == "dgraph":
                internal_schema = KVRocksInternalSchema("dgraph")
                app.include_router(dgraph.router)
            elif datastore == "clickhouse":
                internal_schema = KVRocksInternalSchema( "clickhouse")
                app.include_router(clickhouse.router)
            elif datastore == "arangodb":
                internal_schema = KVRocksInternalSchema( "arangodb")
                app.include_router(arangodb.router)
        
        #check node type (chief or worker )
        if CoreContainer.config.node_type() == "worker":
            excluded_schema_handlers = ["add_clickhouse_collection", "delete_clickhouse_collection",
            "add_dgraph_collection", "delete_dgraph_collection",
            "add_manticore_collection", "delete_manticore_collection",
            "add_arangodb_collection", "delete_arangodb_collection"]
            routes_to_exclude = [r for r in app.routes if r.name in excluded_schema_handlers]
            for x in routes_to_exclude:
                app.routes.remove(x)

        for route in app.routes:
            if isinstance(route, APIRoute):
                route.operation_id = route.name
        uvicorn.run(app, host="0.0.0.0", port=int(CoreContainer.config.rest_port()))

if __name__ == "__main__":
    manager = PolyglotDataManager()