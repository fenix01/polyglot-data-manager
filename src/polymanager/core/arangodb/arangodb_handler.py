
import logging
from pyArango.connection import *
from polymanager.core.graph_handler import GraphHandler
import uuid
import json
from tenacity import retry, wait_fixed, stop_after_attempt

class ArangoDBHandler(GraphHandler):

    @retry(wait=wait_fixed(10), stop=stop_after_attempt(18))
    def get_conn(self):
        return Connection(arangoURL=self.url,
        username=self.user, password=self.password, max_retries=10)
    
    def __init__(
                self,
                hostname="127.0.0.1",
                port="8529",
                user="root",
                password=""
                ):
        self._hostname = hostname
        self._port = port
        self.user = user
        self.password = password
        self.url = "http://{}:{}".format(self._hostname, self._port)
        self.conn = self.get_conn() 
        self._log = logging.getLogger(__name__)

    def get_health(self):
        res = requests.get(self.url+"/_db/_system/_admin/server/availability", timeout=5)
        return json.loads(res.text)

    def create_indexes(self, collection, collection_opts):
        for index in collection_opts["indexes"]:
            index_type = index["index_type"]
            fields = index["fields"]
            name = index["name"]
            if index_type == "persistent":
                unique = index_type["unique"] if "unique" in index_type else False
                sparse = index_type["sparse"] if "sparse" in index_type else True
                deduplicate = index_type["deduplicate"] if "deduplicate" in index_type else False
                collection.ensurePersistentIndex(fields, unique=unique, sparse=sparse, deduplicate=deduplicate, name=name)
            elif index_type == "hash":
                unique = index_type["unique"] if "unique" in index_type else False
                sparse = index_type["sparse"] if "sparse" in index_type else True
                deduplicate = index_type["deduplicate"] if "deduplicate" in index_type else False
                collection.ensureHashIndex(fields, unique=unique, sparse=sparse, deduplicate=deduplicate, name=name)
            elif index_type == "fulltext":
                collection.ensureFulltextIndex(fields, name=name)

    def insert_schema(self, schema):
        db_name = schema.get_namespace()
        if not self.conn.hasDatabase(db_name):
            self.conn.createDatabase(db_name)
        self.db = self.conn[db_name]
        collection = None
        collection_opts = schema.get_global_collection_opts()
        if collection_opts:
            if collection_opts["edge_collection"]:
                collection = self.db.createCollection("Edges", name=schema.get_collection_name())
            else:
                collection = self.db.createCollection(name=schema.get_collection_name())
        else:
            collection = self.db.createCollection(name=schema.get_collection_name())
        
        if collection_opts:
            self.create_indexes(collection, collection_opts)

        
    def drop_all(self):
        for db in self.conn.databases.keys():
            if db == "_system":
                continue
            session = requests.Session()
            session.auth = (self.user, self.password)
            res = session.delete(self.url+"/_api/database/"+db)

    def add_nodes(self, namespace, collection_name, list_nodes, ref_node):
        self.db = self.conn[namespace]
        collection = self.db[collection_name]
        ids = []
        for node in list_nodes:
            doc = collection.createDocument()
            doc.set(node)
            key = str(uuid.uuid4())
            doc._key = key
            doc.save()
            arangodb_id = "{}/{}".format(collection_name, key)
            node = {"_key": key, "_id": arangodb_id}
            ids.append(node)
        return ids

    def add_node(self, namespace, collection_name, node, ref_node):
        self.db = self.conn[namespace]
        collection = self.db[collection_name]
        doc = collection.createDocument()
        doc.set(node)
        key = str(uuid.uuid4())
        doc._key = key
        doc.save()
        arangodb_id = "{}/{}".format(collection_name, key)
        node = {"_key": key, "_id": arangodb_id}
        return node

    def delete_node(self,  namespace, collection_name, node_id):
        self.db = self.conn[namespace]
        collection = self.db[collection_name]
        doc = collection[node_id]
        doc.delete()

    def delete_nodes(self, namespace, collection_name, nodes_id):
        self.db = self.conn[namespace]
        collection = self.db[collection_name]
        for node_id in nodes_id:
            doc = collection[node_id]
            doc.delete()

    def get_predicate(self, namespace, collection_name, node_id, predicate):
        self.db = self.conn[namespace]
        res = self.db.AQLQuery("""RETURN DOCUMENT("{}", "{}")""".format(collection_name, node_id))
        doc = res.response["result"][0]
        if doc:
            return doc[predicate]

    def update_node(self, namespace, collection_name, node_id, node):
        self.db = self.conn[namespace]
        collection = self.db[collection_name]
        doc = collection[node_id]
        doc.set(node)
        doc.save()

    def reset_relationships(self, relationships, node_id):
        pass

    def get_edges(self,namespace, collection_name, node_id):
        try :
            self.db = self.conn[namespace]
            query = '''
            FOR e IN `{}`
            FILTER e._from == '{}' || e._to == '{}'
            RETURN e
            '''.format(collection_name, node_id, node_id)
            queryResult = self.db.AQLQuery(query, rawResults=True)
            return queryResult.response["result"]
        except :
            return None


    def update_relationships(self, namespace, collection_name, relationships):
        try :
            self.db = self.conn[namespace]
            collection = self.db[collection_name]
            id_from = "{}".format(relationships["from"])
            id_to = "{}".format(relationships["to"])
            relationships.pop("from")
            relationships.pop("to")
            edge = collection.fetchFirstExample({"_to": id_to, "_from": id_from})
            if edge.result:
                id_ = edge.result[0]["_key"]
                doc = collection[id_]
                for key in relationships.keys():
                    doc[key] = relationships[key]
                doc.save()
                return {"_key": doc._key, "_id": doc._id}
            _key = str(uuid.uuid4())
            arangodb_id = "{}/{}".format(collection_name, _key)
            doc = collection.createEdge()
            doc._key = _key
            doc.links(id_from, id_to)
            for key in relationships.keys():
                doc[key] = relationships[key]
            doc.save()
            return {"_key": _key, "_id": arangodb_id}
        except Exception as e:
            return None
            
    def delete_relationships(self, namespace, collection_name, edges_id):
        try :
            self.db = self.conn[namespace]
            collection = self.db[collection_name]
            for edge_id in edges_id:
                doc = collection[edge_id]
                doc.delete()
            return True
        except Exception as e:
            return False

    def truncate(self, namespace, collection_name):
        self.db = self.conn[namespace]
        collection = self.db[collection_name]
        collection.truncate()

    def query(self, namespace, query):
        self.db = self.conn[namespace]
        res = self.db.AQLQuery(query)
        return res.response

    def node_exists(self, namespace, collection_name, node_id):
        self.db = self.conn[namespace]
        res = self.db.AQLQuery("""RETURN DOCUMENT("{}", "{}")""".format(collection_name, node_id))
        return res.response