from fastapi import APIRouter, HTTPException
from polymanager.containers import CoreContainer, ArangoDBContainer, DGraphContainer, ClickhouseContainer
import logging
from polymanager.exceptions.status_exception import NotReadyDatabase
router = APIRouter()

def check_arangodb_status():
    arangodb_handler = ArangoDBContainer().handler()
    res = arangodb_handler.get_health()
    if res and res["code"] == 200:
        return True
    else:
        return False

def check_dgraph_status():
    dgraph_handler = DGraphContainer().handler()
    res = dgraph_handler.get_health()
    if res and res[0]["status"] == "healthy":
        return True
    else:
        return False

def check_clikhouse_status():
    clickhouse_handler = ClickhouseContainer().handler()
    res = clickhouse_handler.ping()
    if res == "Ok.\n":
        return True
    else:
        return False

def check_status():
    status = True
    try:
        for datastore in CoreContainer.config.connectors():
            if datastore == "arangodb":
                arangodb_status = check_arangodb_status()
                status = status and arangodb_status
            elif datastore == "dgraph":
                dgraph_status = check_dgraph_status()
                status = status and dgraph_status
            elif datastore == "clickhouse":
                clickhouse_status = check_clikhouse_status()
                status = status and clickhouse_status
            elif datastore == "manticoresearch":
                #manticoresearch doesn't have an health api to check the database status
                pass
        return status
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        return False

@router.get("/health", tags=["global"])
def check_health():
    result = {}
    status = True
    try:
        status = check_status()
        if status:
            result["status"] = "ready"
        else:
            raise NotReadyDatabase()
        return result
    except NotReadyDatabase as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "not ready"
        result["error"] = str(e)
        raise HTTPException(status_code=503, detail=result)
    except Exception as e:
        logger = logging.getLogger()
        logger.exception(e)
        result["status"] = "failed"
        result["error"] = str(e)
        raise HTTPException(status_code=500, detail=result)