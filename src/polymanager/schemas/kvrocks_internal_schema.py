from polymanager.schemas.dgraph.dgraph_schema import DGraphSchema
from polymanager.schemas.manticore.manticoresearch_schema import ManticoreSearchSchema
from polymanager.schemas.clickhouse.clickhouse_schema import ClickhouseSchema
from polymanager.containers import RedisContainer, DGraphContainer, ManticoreContainer, ClickhouseContainer
from polymanager.exceptions.schema_exception import ExistingSchema
import json

class KVRocksInternalSchema:
    "kvrocks is used to store all schemas for various datasources"

    def __init__(self, datastore):
        if datastore not in ["manticoresearch", "dgraph", "clickhouse"]:
            raise Exception("this datastore is not supported")
        self.datastore = datastore

    def load_schemas(self):
        db = RedisContainer.db()
        res = db.hgetall(self.datastore)
        schemas = []
        if res:
            for schema_name in res:
                schema_obj = None
                if self.datastore == "dgraph":
                    schema_obj = DGraphSchema.load_schema(json.loads(res[schema_name]))
                elif self.datastore == "manticoresearch":
                    schema_obj = ManticoreSearchSchema.load_schema(json.loads(res[schema_name]))
                elif self.datastore == "clickhouse":
                    schema_obj = ClickhouseSchema.load_schema(json.loads(res[schema_name]))
                schemas.append(schema_obj)
            return schemas
        else:
            return schemas
        
    def get_schema(self, collection_name):
        db = RedisContainer.db()
        schema = db.hget(self.datastore, collection_name)
        if not schema:
            return None
        if self.datastore == "dgraph":
            return DGraphSchema.load_schema(json.loads(schema))
        elif self.datastore == "manticoresearch":
            return ManticoreSearchSchema.load_schema(json.loads(schema))
        elif self.datastore == "clickhouse":
            return ClickhouseSchema.load_schema(json.loads(schema))

    def populate_database(self, schema):
        if self.datastore == "dgraph":
            dgraph_handler = DGraphContainer().handler()
            dgraph_handler.insert_schema(schema)
        elif self.datastore == "manticoresearch":
            manticore_handler = ManticoreContainer().handler()
            manticore_handler.insert_schema(schema)
        elif self.datastore == "clickhouse":
            clickhouse_handler = ClickhouseContainer().handler()
            clickhouse_handler.insert_schema(schema)

    def delete_database(self, schema):
        if self.datastore == "dgraph":
            dgraph_handler = DGraphContainer().handler()
            #delete all nodes type
            dgraph_handler.delete_nodes_type(schema.get_collection_name())
            dgraph_handler.delete_schema(schema)
        elif self.datastore == "manticoresearch":
            manticore_handler = ManticoreContainer().handler()
            manticore_handler.delete_schema(schema)
        elif self.datastore == "clickhouse":
            clickhouse_handler = ClickhouseContainer().handler()
            clickhouse_handler.delete_schema(schema)
        
    def save_schema(self, schema):
        #validate schema before insert
        exists = self.get_schema(schema.get_collection_name())
        if exists:
            raise ExistingSchema("this collection already exists")

        #add schema to the internal state
        db = RedisContainer.db()
        db.hset(self.datastore, schema.get_collection_name(),json.dumps(schema.get_schema()) )
        
        #populate the schema
        self.populate_database(schema)

    def delete_schema(self, collection_name):
        db = RedisContainer.db()
        schema = self.get_schema(collection_name)
        if schema:
            # #remove all predicates, types, tables of this schema
            
            self.delete_database(schema)
            
            #remove schema from internals
            db.hdel(self.datastore, schema.get_collection_name())

