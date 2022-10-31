from polymanager.schemas.kvrocks_internal_schema import KVRocksInternalSchema
from polymanager.containers import ArangoDBContainer
from polymanager.exceptions.schema_exception import UnkownSchema, InvalidSchema
from polymanager.schemas.graph_collection import GraphCollection
import uuid
class ArangodbCollection(GraphCollection):

    def is_valid_uuid(self, val):
        try:
            uuid.UUID(str(val))
            return True
        except ValueError:
            return False

    def add_node(self, node):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()

        #checking schema before insert
        self.arangodb_schema.check_node_schema(node)
        
        #adding node to arangodb
        uid = arangodb_handler.add_node(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name(),
            self.arangodb_schema.get_node_schema(node), "new_node")
        result["node_id"] = uid
        return result

    def add_nodes(self, nodes):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()

        #checking schema before insert
        for _node in nodes:
            self.arangodb_schema.check_node_schema(_node)
            
        #adding node to arangodb
        nodes_schema = []
        for _node in nodes:
            nodes_schema.append(self.arangodb_schema.get_node_schema(_node))
        uids = arangodb_handler.add_nodes(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name(),
            nodes_schema,
            "new_node")
        result["nodes_id"] = uids
        return result

    def delete_node(self, node_id):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()
        if self.is_valid_uuid(node_id):
            arangodb_handler.delete_node(
                self.arangodb_schema.get_namespace(),
                self.arangodb_schema.get_collection_name(),
                node_id
            )
            result["status"] = "success"
            return result
        else:
            raise InvalidSchema("node_id should be a uuid4 string")

    def delete_nodes(self, nodes_id):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()
        #checking schema before delete
        for _node in nodes_id:
            if not self.is_valid_uuid(_node):
                raise InvalidSchema("nodes_id should be a list of uuid4 string")
        #delete documents
        arangodb_handler.delete_nodes(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name(),
            nodes_id
        )
        result["status"] = "success"
        return result

    def update_node(self, node_id, node):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()

        #checking schema before insert
        self.arangodb_schema.check_node_schema(node)
        
        #updating the node
        uid = arangodb_handler.update_node(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name(),
            node_id,
            self.arangodb_schema.get_node_schema(node)
            )
        result["status"] = "success"
        return result

    def update_relationship(self, edge, reset=False):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()

        #checking schema before insert
        self.arangodb_schema.check_relationship_schema(edge)
        
        #updating the node
        if reset:
            arangodb_handler.reset_relationships(self.arangodb_schema.get_relationship_fields())
        edge = arangodb_handler.update_relationships(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name(),
            self.arangodb_schema.get_edge_schema(edge),
        )
        result["edge_id"] = edge
        return result

    def delete_relationships(self, edges_id):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()
        
        arangodb_handler.delete_relationships(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name(),
            edges_id,
        )
        result["status"] = "success"
        return result  

    def truncate(self):
        result = {}
        arangodb_handler = ArangoDBContainer().handler()
        #delete all nodes type
        arangodb_handler.truncate(
            self.arangodb_schema.get_namespace(),
            self.arangodb_schema.get_collection_name())
        result["status"] = "success"
        return result

    def __init__(self, internal_schema: KVRocksInternalSchema, collection_name):
        self.internal_schema = internal_schema
        exists = self.internal_schema.get_schema(collection_name)
        if not exists:
            raise UnkownSchema("this collection does not exist")
        self.arangodb_schema = exists