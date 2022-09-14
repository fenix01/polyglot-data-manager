from abc import ABC, abstractmethod


class GraphCollection(ABC):
    
    @abstractmethod
    def add_node(self, node):
        pass

    @abstractmethod
    def add_nodes(self, nodes):
        pass

    @abstractmethod
    def delete_node(self, node_id):
        pass

    @abstractmethod
    def delete_nodes(self, nodes_id):
        pass

    @abstractmethod
    def update_node(self, node_id, node):
        pass

    @abstractmethod
    def update_relationship(self, node_id, node, reset=False):
        pass

    @abstractmethod
    def truncate(self):
        pass