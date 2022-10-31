from abc import ABC, abstractmethod


class GraphHandler(ABC):
    
    @abstractmethod
    def add_nodes(self, namespace, collection_name, list_nodes, ref_node):
        pass

    @abstractmethod
    def add_node(self, namespace, collection_name, node, ref_node):
        pass

    @abstractmethod
    def delete_node(self, namespace, collection_name, node_id):
        pass

    @abstractmethod
    def delete_nodes(self, namespace, collection_name, nodes_id):
        pass

    @abstractmethod
    def update_node(self, namespace, collection_name, node_id, node):
        pass

    @abstractmethod
    def reset_relationships(self, namespace, collection_name, relationships, node_id):
        pass

    @abstractmethod
    def update_relationships(self, namespace, collection_name, relationships, node, node_id):
        pass

    @abstractmethod
    def delete_relationships(self, namespace, collection_name, edges_id):
        pass

    @abstractmethod
    def truncate(self, namespace, collection_name):
        pass

    @abstractmethod
    def node_exists(self, namespace, collection_name, node_id):
        pass