from abc import ABC, abstractmethod


class DocumentHandler(ABC):
    
    @abstractmethod
    def bulk_add_documents(self, collection_name, documents_list):
        pass

    @abstractmethod
    def bulk_replace_documents(self, collection_name, documents_list):
        pass

    @abstractmethod
    def delete_document(self, collection_name, doc_id):
        pass

    @abstractmethod
    def delete_documents(self, collection_name, docs_id):
        pass

    @abstractmethod
    def truncate(self, collection_name):
        pass