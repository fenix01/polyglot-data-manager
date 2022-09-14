from fastapi import APIRouter, HTTPException
from typing import Optional
from polymanager.routers.graph_models import *
from polymanager.exceptions.schema_exception import UnkownSchema, ExistingSchema, InvalidSchema
from polymanager.schemas.dgraph.dgraph_schema import DGraphSchema
from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.dgraph.dgraph_collection import DgraphCollection
import logging
router = APIRouter()



@router.get("/collection/dgraph", tags=["dgraph"])
def show_dgraph_collections():
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "dgraph")
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

@router.post("/collection/dgraph", tags=["dgraph"])
def add_dgraph_collection(collection: Collection):
    result = {}
    internal_schema = KVRocksInternalSchema( "dgraph")
    schema = DGraphSchema(collection.namespace, collection.collection, collection.field_value)
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

@router.delete("/collection/dgraph", tags=["dgraph"])
def delete_dgraph_collection(collection: CollectionDel):
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "dgraph")
        collection_name = "{}.{}".format(collection.namespace, collection.collection)
        schema = internal_schema.get_schema(collection_name)
        if not schema:
            raise UnkownSchema("this collection does not exist")
        if collection.empty:
            node_collection = DgraphCollection(internal_schema, collection_name)
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

@router.post("/collection/dgraph/nodes", tags=["dgraph"])
def add_dgraph_handler(node: Nodes):
    try:
        result = {}
        collection_name = "{}.{}".format(node.namespace, node.collection)
        internal_schema = KVRocksInternalSchema( "dgraph")
        node_collection = DgraphCollection(internal_schema, collection_name)
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


@router.post("/collection/dgraph/node", tags=["dgraph"])
def add_dgraph_node(node: Node):
    try:
        result = {}
        collection_name = "{}.{}".format(node.namespace, node.collection)
        internal_schema = KVRocksInternalSchema( "dgraph")
        node_collection = DgraphCollection(internal_schema, collection_name)
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

@router.put("/collection/dgraph/relationship", tags=["dgraph"])
def update_dgraph_relationship(node: UpdateRelationship):
    try:
        result = {}
        collection_name = "{}.{}".format(node.namespace, node.collection)
        internal_schema = KVRocksInternalSchema( "dgraph")
        node_collection = DgraphCollection(internal_schema, collection_name)
        result = node_collection.update_relationship(node.node_id, node.node, node.reset)
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

@router.delete("/collection/dgraph/nodes", tags=["dgraph"])
def delete_dgraph_handler(node: DelNodes):
    try:
        result = {}
        collection_name = "{}.{}".format(node.namespace, node.collection)
        internal_schema = KVRocksInternalSchema( "dgraph")
        node_collection = DgraphCollection(internal_schema, collection_name)
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

@router.delete("/collection/dgraph/node", tags=["dgraph"])
def delete_dgraph_node(node: DelNode):
    try:
        result = {}
        collection_name = "{}.{}".format(node.namespace, node.collection)
        internal_schema = KVRocksInternalSchema( "dgraph")
        node_collection = DgraphCollection(internal_schema, collection_name)
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

@router.put("/collection/dgraph/node", tags=["dgraph"])
def update_dgraph_node(node: UpdateNode):
    try:
        result = {}
        collection_name = "{}.{}".format(node.namespace, node.collection)
        internal_schema = KVRocksInternalSchema( "dgraph")
        node_collection = DgraphCollection(internal_schema, collection_name)
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