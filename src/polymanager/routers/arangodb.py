from fastapi import APIRouter, HTTPException
from typing import Optional
from polymanager.routers.graph_models import *
from polymanager.exceptions.schema_exception import UnkownSchema, ExistingSchema, InvalidSchema
from polymanager.schemas.arangodb.arangodb_schema import ArangodbSchema
from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.arangodb.arangodb_collection import ArangodbCollection
import logging
router = APIRouter()



@router.get("/collection/arangodb", tags=["arangodb"])
def show_arangodb_collections():
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "arangodb")
        schema_objs = internal_schema.load_schemas()
        res = []
        for schema_obj in schema_objs:
            res.append(schema_obj.get_schema())
        result["schemas"] = res
        result["status"] = "success"
        return result
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.post("/collection/arangodb", tags=["arangodb"])
def add_arangodb_collection(collection: Collection):
    result = {}
    internal_schema = KVRocksInternalSchema( "arangodb")
    schema = ArangodbSchema(collection.namespace, collection.collection, collection.field_value, global_collection_opts=collection.global_options)
    try:
        #save schema in graph database
        internal_schema.save_schema(schema)
        result["status"] = "success"
        return result
    except ExistingSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=409, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.delete("/collection/arangodb", tags=["arangodb"])
def delete_arangodb_collection(collection: CollectionDel):
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "arangodb")
        collection_name = "{}".format(collection.collection)
        schema = internal_schema.get_schema(collection_name)
        if not schema:
            raise UnkownSchema("this collection does not exist")
        if collection.empty:
            node_collection = ArangodbCollection(internal_schema, collection_name)
            node_collection.truncate()
        else:
            internal_schema.delete_schema(schema.get_collection_name())
        result["status"] = "success"
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.post("/collection/arangodb/nodes", tags=["arangodb"])
def add_arangodb_handler(node: Nodes):
    try:
        result = {}
        collection_name = "{}".format(node.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        node_collection = ArangodbCollection(internal_schema, collection_name)
        result = node_collection.add_nodes(node.nodes)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except InvalidSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=400, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)


@router.post("/collection/arangodb/node", tags=["arangodb"])
def add_arangodb_node(node: Node):
    try:
        result = {}
        collection_name = "{}".format(node.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        node_collection = ArangodbCollection(internal_schema, collection_name)
        result = node_collection.add_node(node.node)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except InvalidSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=400, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.put("/collection/arangodb/relationship", tags=["arangodb"])
def update_arangodb_relationship(edge: UpdateEdge):
    try:
        result = {}
        collection_name = "{}".format(edge.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        edge_collection = ArangodbCollection(internal_schema, collection_name)
        result = edge_collection.update_relationship(edge.edge, edge.reset)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except InvalidSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=400, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.delete("/collection/arangodb/relationships", tags=["arangodb"])
def delete_arangodb_relationships(edges: DelEdges):
    try:
        result = {}
        collection_name = "{}".format(edges.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        node_collection = ArangodbCollection(internal_schema, collection_name)
        result = node_collection.delete_relationships(edges.edges_id)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.delete("/collection/arangodb/nodes", tags=["arangodb"])
def delete_arangodb_handler(node: DelNodes):
    try:
        result = {}
        collection_name = "{}".format(node.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        node_collection = ArangodbCollection(internal_schema, collection_name)
        result = node_collection.delete_nodes(node.nodes_id)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except InvalidSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=400, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.delete("/collection/arangodb/node", tags=["arangodb"])
def delete_arangodb_node(node: DelNode):
    try:
        result = {}
        collection_name = "{}".format(node.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        node_collection = ArangodbCollection(internal_schema, collection_name)
        result = node_collection.delete_node(node.node_id)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except InvalidSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=400, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)

@router.put("/collection/arangodb/node", tags=["arangodb"])
def update_arangodb_node(node: UpdateNode):
    try:
        result = {}
        collection_name = "{}".format(node.collection)
        internal_schema = KVRocksInternalSchema( "arangodb")
        node_collection = ArangodbCollection(internal_schema, collection_name)
        result = node_collection.update_node(node.node_id, node.node)
        return result
    except UnkownSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=404, detail=result)
    except InvalidSchema as e:
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=400, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)