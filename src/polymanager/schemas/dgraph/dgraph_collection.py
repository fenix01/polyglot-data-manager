from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.containers import DGraphContainer
from polymanager.exceptions.schema_exception import UnkownSchema, InvalidSchema
from polymanager.schemas.graph_collection import GraphCollection
import json

class DgraphCollection(GraphCollection):

    def add_node(self, node):
        result = {}
        dgraph_handler = DGraphContainer().handler()
        #manticore_handler = ManticoreContainer().handler()

        #checking schema before insert
        self.dgraph_schema.check_node_schema(node)
        
        #adding node to dgraph
        uid = dgraph_handler.add_node(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            self.dgraph_schema.get_node_schema(node), "new_node")
        result["node_id"] = uid
        return result

    def add_nodes(self, nodes):
        result = {}
        dgraph_handler = DGraphContainer().handler()

        #checking schema before insert
        for _node in nodes:
            self.dgraph_schema.check_node_schema(_node)
            
        #adding node to dgraph and manticore
        nodes_schema = []
        for _node in nodes:
            nodes_schema.append(self.dgraph_schema.get_node_schema(_node))
        uids = dgraph_handler.add_nodes(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            nodes_schema, "new_node")
        result["nodes_id"] = uids
        return result

    def delete_node(self, node_id):
        result = {}
        dgraph_handler = DGraphContainer().handler()
        try:
            node_id_int = int(node_id, 16)
            uid = dgraph_handler.delete_node(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            node_id)
            result["status"] = "success"
            return result
        except Exception as e:
            raise InvalidSchema("node_id should be a hex string")

    def delete_nodes(self, nodes_id):
        result = {}
        dgraph_handler = DGraphContainer().handler()
        #checking schema before delete
        int_nodes = []
        for _node in nodes_id:
            try:
                node_id_int = int(_node, 16)
                int_nodes.append(node_id_int)
            except Exception as e:
                raise InvalidSchema("nodes_id should be a list of hex string")
        string_int_nodes = [str(int_node) for int_node in int_nodes]
        #delete documents
        uid = dgraph_handler.delete_nodes(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            nodes_id)
        result["status"] = "success"
        return result

    def update_node(self, node_id, node):
        result = {}
        dgraph_handler = DGraphContainer().handler()

        #checking schema before insert
        self.dgraph_schema.check_node_schema(node)
        
        #updating the node
        uid = dgraph_handler.update_node(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            node_id,
            self.dgraph_schema.get_node_schema(node))
        result["status"] = "success"
        return result

    def update_relationship(self, node_id, node, reset=False):
        result = {}
        dgraph_handler = DGraphContainer().handler()

        #checking schema before insert
        self.dgraph_schema.check_relationship_schema(node)
        
        #updating the node
        if reset:
            dgraph_handler.reset_relationships(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            self.dgraph_schema.get_relationship_fields(),
            node_id)
        dgraph_handler.update_relationships(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name(),
            self.dgraph_schema.get_relationship_fields(),
            self.dgraph_schema.get_node_schema(node),
            node_id)
        result["node_id"] = node_id
        return result

    def truncate(self):
        result = {}
        dgraph_handler = DGraphContainer().handler()
        #delete all nodes type
        dgraph_handler.truncate(
            self.dgraph_schema.get_namespace(),
            self.dgraph_schema.get_collection_name())
        result["status"] = "success"
        return result

    def __init__(self, internal_schema: KVRocksInternalSchema, collection_name):
        self.internal_schema = internal_schema
        exists = self.internal_schema.get_schema(collection_name)
        if not exists:
            raise UnkownSchema("this collection does not exist")
        self.dgraph_schema = exists