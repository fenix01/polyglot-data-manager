from fastapi import APIRouter, HTTPException
from fastapi import HTTPException
from polymanager.routers.document_models import *
from polymanager.exceptions.schema_exception import UnkownSchema, ExistingSchema, InvalidSchema
from polymanager.schemas.clickhouse.clickhouse_schema import ClickhouseSchema
from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.clickhouse.clickhouse_collection import ClickhouseCollection
import logging

router = APIRouter()

@router.get("/collection/clickhouse", tags=["clickhouse"])
def show_clickhouse_collections():
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "clickhouse")
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

@router.post("/collection/clickhouse", tags=["clickhouse"])
def add_clickhouse_collection(collection: Collection):
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "clickhouse")
        schema = ClickhouseSchema(
            collection.namespace,
            collection.collection,
            collection.field_value,
            global_collection_opts=collection.global_options)
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

@router.delete("/collection/clickhouse", tags=["clickhouse"])
def delete_clickhouse_collection(collection: CollectionDel):
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "clickhouse")
        collection_name = "{}.{}".format(collection.namespace, collection.collection)
        schema = internal_schema.get_schema(collection_name)
        if not schema:
            raise UnkownSchema("this collection does not exist")
        if collection.empty:
            document_collection = ClickhouseCollection(internal_schema, collection_name)
            document_collection.truncate()
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

@router.post("/collection/clickhouse/documents", tags=["clickhouse"])
def add_clickhouse_documents(document: Documents):
    result = {}
    try:
        collection_name = "{}.{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "clickhouse")
        document_collection = ClickhouseCollection(internal_schema, collection_name)
        result = document_collection.add_documents(document.documents)
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


@router.post("/collection/clickhouse/document", tags=["clickhouse"])
def add_clickhouse_document(document: Document):
    result = {}
    try:
        collection_name = "{}.{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "clickhouse")
        document_collection = ClickhouseCollection(internal_schema, collection_name)
        result = document_collection.add_document(document.document)
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


@router.delete("/collection/clickhouse/documents", tags=["clickhouse"])
def delete_clickhouse_documents(document: DelDocuments):
    result = {}
    try:
        collection_name = "{}.{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "clickhouse")
        document_collection = ClickhouseCollection(internal_schema, collection_name)
        result = document_collection.delete_documents(document.documents_id)
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

@router.delete("/collection/clickhouse/document", tags=["clickhouse"])
def delete_clickhouse_document(document: DelDocument):
    result = {}
    try:
        collection_name = "{}.{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "clickhouse")
        document_collection = ClickhouseCollection(internal_schema, collection_name)
        result = document_collection.delete_document(document.document_id)
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

@router.put("/collection/clickhouse/document", tags=["clickhouse"])
def update_clickhouse_document(document: UpdateDocument):
    result = {}
    try:
        collection_name = "{}.{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "clickhouse")
        document_collection = ClickhouseCollection(internal_schema, collection_name)
        result = document_collection.update_document(document.document)
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