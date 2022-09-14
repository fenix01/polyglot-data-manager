from polymanager.containers import ManticoreContainer
from polymanager.exceptions.schema_exception import UnkownSchema, InvalidSchema
from polymanager.schemas.document_collection import DocumentCollection

class ManticoreSearchCollection(DocumentCollection):

    def add_document(self, document):
        result = {}
        manticore_handler = ManticoreContainer().handler()

        #checking schema before insert
        self.schema.check_document_schema(document)

        #adding document to manticore
        index_document = self.schema.get_document_schema(document)
        manticore_handler.bulk_add_documents(self.schema.get_collection_name(),[index_document])
        result["status"] = "success"
        return result

    def add_documents(self, documents):
        result = {}
        manticore_handler = ManticoreContainer().handler()

        #checking schema before insert
        for _document in documents:
            self.schema.check_document_schema(_document)
        #adding document to dgraph and manticore
        documents_schema = []
        for _document in documents:
            index_document = self.schema.get_document_schema(_document)
            documents_schema.append(index_document)
        manticore_handler.bulk_add_documents(self.schema.get_collection_name(), documents_schema)
        result["status"] = "success"
        return result

    def delete_document(self, document_id):
        result = {}
        manticore_handler = ManticoreContainer().handler()
        manticore_handler.delete_document(self.schema.get_collection_name(), document_id)
        result["status"] = "success"
        return result

    def delete_documents(self, documents_id):
        result = {}
        manticore_handler = ManticoreContainer().handler()
        int_documents = []
        for _document in documents_id:
            if isinstance(_document, int):
                int_documents.append(str(_document))
            else:
                raise InvalidSchema("documents_id should be a list of int")
        #delete documents
        manticore_handler.delete_documents(self.schema.get_collection_name(), int_documents)
        result["status"] = "success"
        return result

    def update_document(self, document):
        result = {}
        manticore_handler = ManticoreContainer().handler()

        #checking schema before insert
        self.schema.check_document_schema(document)
        #updating the document
        index_document = self.schema.get_document_schema(document)
        #index_document["id"] = document_id
        manticore_handler.bulk_replace_documents(self.schema.get_collection_name(),[index_document])
        result["status"] = "success"
        return result

    def truncate(self):
        result = {}
        manticore_handler = ManticoreContainer().handler()
        manticore_handler.truncate(self.schema.get_collection_name())
        result["status"] = "success"
        return result

    def __init__(self, internal_schema, collection_name):
        self.internal_schema = internal_schema
        exists = self.internal_schema.get_schema(collection_name)
        if not exists:
            raise UnkownSchema("this collection does not exist")
        self.schema = exists