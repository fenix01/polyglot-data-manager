from fastapi import APIRouter, HTTPException
from polymanager.routers.document_models import *
from polymanager.exceptions.schema_exception import UnkownSchema, ExistingSchema, InvalidSchema
from polymanager.schemas.manticore.manticoresearch_schema import ManticoreSearchSchema
from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.schemas.manticore.manticore_collection import ManticoreSearchCollection
import logging

router = APIRouter()

@router.get("/collection/manticoresearch", tags=["manticoresearch"])
def show_manticore_collections():
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "manticoresearch")
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

@router.post("/collection/manticoresearch", tags=["manticoresearch"])
def add_manticore_collection(collection: Collection):
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        if collection.global_options:
            schema = ManticoreSearchSchema(collection.namespace, collection.collection, collection.field_value, global_collection_opts=collection.global_options)
        else:
            schema = ManticoreSearchSchema(collection.namespace, collection.collection, collection.field_value)
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

@router.delete("/collection/manticoresearch", tags=["manticoresearch"])
def delete_manticore_collection(collection: CollectionDel):
    result = {}
    try:
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        collection_name = "{}_{}".format(collection.namespace, collection.collection)
        schema = internal_schema.get_schema(collection_name)
        if not schema:
            raise UnkownSchema("this collection does not exist")
        if collection.empty:
            document_collection = ManticoreSearchCollection(internal_schema, collection_name)
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

@router.post("/collection/manticoresearch/documents", tags=["manticoresearch"])
def add_manticore_documents(document: Documents):
    result = {}
    try:
        collection_name = "{}_{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        document_collection = ManticoreSearchCollection(internal_schema, collection_name)
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


@router.post("/collection/manticoresearch/document", tags=["manticoresearch"])
def add_manticore_document(document: Document):
    result = {}
    try:
        collection_name = "{}_{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        document_collection = ManticoreSearchCollection(internal_schema, collection_name)
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


@router.delete("/collection/manticoresearch/documents", tags=["manticoresearch"])
def delete_manticore_documents(document: DelDocuments):
    result = {}
    try:
        collection_name = "{}_{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        document_collection = ManticoreSearchCollection(internal_schema, collection_name)
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

@router.delete("/collection/manticoresearch/document", tags=["manticoresearch"])
def delete_manticore_document(document: DelDocument):
    result = {}
    try:
        collection_name = "{}_{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        document_collection = ManticoreSearchCollection(internal_schema, collection_name)
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

@router.put("/collection/manticoresearch/document", tags=["manticoresearch"])
def update_manticore_document(document: UpdateDocument):
    result = {}
    try:
        collection_name = "{}_{}".format(document.namespace, document.collection)
        internal_schema = KVRocksInternalSchema( "manticoresearch")
        document_collection = ManticoreSearchCollection(internal_schema, collection_name)
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