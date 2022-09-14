from abc import ABC, abstractmethod


class DocumentCollection(ABC):
    
    @abstractmethod
    def add_document(self, document):
        pass

    @abstractmethod
    def add_documents(self, documents):
        pass

    @abstractmethod
    def delete_document(self, document_id):
        pass

    @abstractmethod
    def delete_documents(self, documents_id):
        pass
    
    @abstractmethod
    def update_document(self, document):
        pass

    @abstractmethod
    def truncate(self):
        pass