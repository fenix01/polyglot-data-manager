import pydgraph
import logging
import json
import requests
from polymanager.core.graph_handler import GraphHandler

class DGraphHandler(GraphHandler):
    # This as possible module to translate need to dgraph
    # language. Must hold as less as possible any check.

    def __init__(self, dgraph_conn, dgraph_admin=["127.0.0.1"], dgraph_admin_port=8080):
        # address is the ip or the dns of the database instance
        # port is the tcp port of the communication
        self.url = "http://" + dgraph_admin[0] + ":" + str(dgraph_admin_port) + "/alter?runInBackground=false"
        self.url_health = "http://" + dgraph_admin[0] + ":" + str(dgraph_admin_port) + "/health"
        self.client = dgraph_conn.get_client()
        self._log = logging.getLogger(__name__)

    def delete_schema(self, schema):
        for key, options in schema.get_fields().items():
            res = requests.post(self.url,data=json.dumps({"drop_attr": key}))
        res = requests.post(self.url,data=json.dumps({"drop_op": "TYPE", "drop_value": schema.get_collection_name()}))
        if res.status_code != 200:
            raise Exception("error while deleting the schema")
    
    def insert_schema(self, schema):
        gen_chema = schema.generate_schema()
        res = requests.post(self.url, data=gen_chema)
        if res.status_code != 200:
            raise Exception("error while inserting the schema")

    def get_health(self):
        res = requests.get(self.url_health, timeout=5)
        return json.loads(res.text)
        
    def dump_schema(self):
        query = "schema{}"
        res = self.client.txn(read_only=True).query(query)
        json_res = json.loads(res.json)
        return json_res

    def drop_all(self):
        return self.client.alter(pydgraph.Operation(drop_all=True))

    def node_exists(self, namespace, collection_name, node_id):
        query = "query all($value: string) {{\
            q(func:uid({})) @filter(type({})) {{\
                uid\
            }}\
        }}".format(node_id, collection_name)
        res = self.client.txn(read_only=True).query(query)
        json_res = json.loads(res.json)
        if len(json_res["q"]) > 0:
            return json_res["q"][0]
        else:
            return None

    def has_node(self, node_type, predicate, value):
        query = "query all($value: string) {{\
            q(func:eq({}, $value)) @filter(type({})) {{\
                uid\
            }}\
        }}".format(predicate, node_type, value)
        variables = {'$value': value}
        res = self.client.txn(read_only=True).query(query, variables=variables)
        json_res = json.loads(res.json)
        if len(json_res["q"]) > 0:
            return json_res["q"][0]
        else:
            return None

    def query(self, query, variables = None):
        if variables:
            res = self.client.txn(read_only=True).query(query, variables = variables)
        else:
            res = self.client.txn(read_only=True).query(query)
        json_res = json.loads(res.json)
        return json_res

    def add_nodes(self, namespace, collection_name, list_predicates, ref_node):
        txn = self.client.txn()
        try:
            res = []
            for predicates in list_predicates:
                obj = {
                    "uid": "_:{}".format(ref_node),
                    "dgraph.type": collection_name,
                }
                obj.update(predicates)
                response = txn.mutate(set_obj=obj)
                res.append(response.uids[ref_node])
            txn.commit()
            return res
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def add_node(self, namespace, collection_name, predicates, ref_node):
        obj = {
            "uid": "_:{}".format(ref_node),
            "dgraph.type": collection_name,
        }
        obj.update(predicates)
        txn = self.client.txn()
        try:
            response = txn.mutate(set_obj=obj)
            txn.commit()
            self._log.info("add new node {}:{}".format(collection_name, predicates))
            return response.uids[ref_node]
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def truncate(self, namespace, collection_name):
        query = """{{
            nodes as var (func: eq(dgraph.type, "{}")) {{
                uid
            }}
        }}""".format(collection_name)
        nquad = """
        uid(nodes) * *  .
        """
        txn = self.client.txn()
        try:
            mutation = txn.create_mutation(del_nquads=nquad)
            request = txn.create_request(query=query, mutations=[mutation], commit_now=True)
            txn.do_request(request)
            self._log.info("delete nodes for {}".format(collection_name))
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def delete_nodes(self, namespace, collection_name, nodes_id):
        txn = self.client.txn()
        try:
            res = []
            for node_id in nodes_id:
                obj = {
                    "uid": node_id,
                    "dgraph.type": collection_name
                }
                response = txn.mutate(del_obj=obj)
                res.append(response.uids[node_id])
            txn.commit()
            return res
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def delete_node(self, namespace, collection_name, node_id):
        obj = {
            "uid": node_id,
            "dgraph.type": collection_name
        }
        txn = self.client.txn()
        try:
            response = txn.mutate(del_obj=obj)
            txn.commit()
            self._log.info("delete node {}".format(node_id))
            return response.uids[node_id]
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def update_node(self, namespace, collection_name, node_id, predicates):
        obj = {
            "uid": node_id
        }
        obj.update(predicates)
        txn = self.client.txn()
        try:
            response = txn.mutate(set_obj=obj)
            txn.commit()
            self._log.info("updated node {} with {}".format(node_id, predicates))
            return response.uids
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def get_predicate_edges(self, uid, predicate):
        query = "query all($uid: string) {{\
            q(func: uid($uid)) {{\
                uid,\
                {} {{ \
                    uid \
                }}\
            }}\
        }}".format(predicate)
        variables = {'$uid': uid}
        res = self.client.txn(read_only=True).query(query, variables=variables)
        json_res = json.loads(res.json)
        return json_res["q"]

    def remove_edges(self, uid, predicate):
        node = self.get_predicate_edges(uid, predicate)[0]
        obj = []
        if predicate not in node:
            return True
        for node_uid in node[predicate]:
            sub_obj = {
                "uid": uid,
                predicate : {
                    "uid": node_uid["uid"]
                }
            }
            obj.append(sub_obj)
        txn = self.client.txn()
        try:
            response = txn.mutate(del_obj=obj)
            txn.commit()
            return True
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def delete_edge(self, source_node, predicate, dest_node):
        obj = {
            "uid": source_node,
            predicate: {
                "uid": dest_node
            }
        }
        txn = self.client.txn()
        try:
            response = txn.mutate(del_obj=obj)
            txn.commit()
            self._log.info("delete edge between {} and {}".format(source_node, dest_node))
            return response.uids
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def get_predicate(self, uid, predicate):
        query = "query all($uid: string) {{\
            q(func: uid($uid)) {{\
                uid,\
                {}\
            }}\
        }}".format(predicate)
        variables = {'$uid': uid}
        res = self.client.txn(read_only=True).query(query, variables=variables)
        json_res = json.loads(res.json)
        return json_res["q"][0]

    def get_uid_from_entities(self, predicate, node_type, entities):
        query = "query all($entities: string) {{\
            q(func: anyofterms({}, $entities)) @filter(type({})) {{\
                uid\
            }}\
        }}".format(predicate, node_type)
        variables = {
            "$entities": entities
            }
        res = self.client.txn(read_only=True).query(query, variables=variables)
        json_res = json.loads(res.json)
        return json_res["q"]

    def update_relationships(self, namespace, collection_name, relationships, node, node_id):
        if len(relationships) == 0:
            return
        for relationship in relationships:
            self.update_edges(node_id, relationship, node[relationship])

    def reset_relationships(self, namespace, collection_name, relationships, node_id):
        if len(relationships) == 0:
            return
        for relationship in relationships:
            self.remove_edges(node_id, relationship)

    def update_edges(self, uid, predicate, entities_uid):
        obj = []
        for entity_uid in entities_uid:
            sub_obj = {
                "uid": uid,
                predicate : {
                    "uid": entity_uid
                }
            }
            obj.append(sub_obj)
        txn = self.client.txn()
        try:
            response = txn.mutate(set_obj=obj)
            txn.commit()
            return True
        except Exception as e:
            self._log.exception(e)
        finally:
            txn.discard()

    def has_predicate_value(self, node_type, predicate, value):
        query = "query all($value: string) {{\
            q(func:eq({}, $value)) @filter(type({})) {{\
                uid\
            }}\
        }}".format(predicate, node_type)
        variables = {'$value': value}
        res = self.client.txn(read_only=True).query(query, variables=variables)
        json_res = json.loads(res.json)
        if len(json_res["q"]) > 0:
            return json_res["q"][0]
        else:
            return None

    def delete_relationships(self, namespace, collection_name, edges_id):
        pass