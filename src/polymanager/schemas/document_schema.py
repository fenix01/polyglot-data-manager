from abc import ABC, abstractmethod


class DocumentSchema(ABC):
    
    @abstractmethod
    def check_global_options(self):
        pass

    @abstractmethod
    def check_fields(self):
        pass

    @abstractmethod
    def parse_field(self, field_type, field_value):
        pass

    @abstractmethod
    def get_schema(self):
        pass

    @abstractmethod
    def load_schema(schema):
        pass

    @abstractmethod
    def get_document_schema(self, document):
        pass

    @abstractmethod
    def check_document_schema(self, document):
        pass

    @abstractmethod
    def generate_schema(self):
        pass

    @abstractmethod
    def get_collection_name(self):
        pass

    @abstractmethod
    def get_fields(self):
        pass

    @abstractmethod
    def get_field(self):
        pass