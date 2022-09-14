

from tokenize import String
from pydantic import create_model, StrictStr, StrictInt, StrictFloat, Json, validator
from dateutil.parser import parse
from datetime import datetime
from typing import (
    Dict, Optional
)
from pydantic import BaseModel
from polymanager.exceptions.schema_exception import InvalidSchema
from polymanager.schemas.document_schema import DocumentSchema
class Index(BaseModel):
    stored: Optional[bool] = None
    class Config:
        extra = "forbid"

class Field(BaseModel):
    type_value: str = ""
    index: Optional[Index] = None
    @validator('type_value')
    def type_in_supported_list(cls, v):
        if v not in ["int", "float", "text", "timestamp", "json"]:
            raise ValueError('{} is not a supported type'.format(v))
        return v

    class Config:
        fields = {'type_value': 'type'}
        extra = "forbid"

class SupportedFields(BaseModel):
    fields_value: Dict[str, Field]

    @validator('fields_value')
    def fields_contains_id(cls, v):
        if "id" not in v.keys():
            raise ValueError('{} requires a id field')
        elif v["id"].type_value != "int":
            raise ValueError('id field must be of type int')
        return v

    class Config:
        fields = {'fields_value': 'fields'}

class ManticoreSearchSchema(DocumentSchema):

    def check_global_options(self):
        pass

    def get_document_schema(self, document):
        new_document = {}
        for key, value in document.items():
            field_options = self.fields[key]
            new_key = "{}{}{}".format(self.namespace, self.namespace_separator, key)
            new_value = self.parse_field(field_options["type"], value)
            if key == "id":
                new_document["id"] = new_value
            else:
                new_document[new_key] = new_value
        return new_document

    def parse_field(self, field_type, field_value):
        if field_type == "text":
            return field_value
        elif field_type == "int":
            return field_value
        elif field_type == "float":
            return field_value
        elif field_type == "timestamp":
            return int(parse(field_value).timestamp())
        elif field_type == "json":
            return field_value


    def check_document_schema(self, document):
        attrs_scheme = dict()
        for key, options in self.fields.items():
            if options["type"] == "text":
                attrs_scheme[key] = (StrictStr, ...)
            elif options["type"] == "int":
                attrs_scheme[key] = (StrictInt, ...)
            elif options["type"] == "float":
                attrs_scheme[key] = (StrictFloat, ...)
            elif options["type"] == "timestamp":
                attrs_scheme[key] = (datetime, ...)
            elif options["type"] == "json":
                attrs_scheme[key] = (Json, ...)
        document_model = create_model('DynamicNodeModel', **attrs_scheme)
        document_model.__config__.extra = 'forbid'
        document_model.__config__.validate_all = True
        try:
            document_model(**document)
        except Exception as e:
            raise InvalidSchema(e)

    def get_fields(self):
        new_fields = {}
        for key, options in self.fields.items():
            options = self.fields[key]
            new_key = "{}{}{}".format(self.namespace, self.namespace_separator, key)
            if key == "id":
                new_fields["id"] = options
            else:
                new_fields[new_key] = options
        return new_fields

    def get_field(self, field):
        return "{}{}{}".format(self.namespace, self.namespace_separator, field)

    def generate_schema(self):
        sql = ""
        attrs_handled = 0
        fields = self.get_fields()
        for index, key in enumerate(fields):
            if key == "id":
                continue
            options = fields[key]
            _type = ""
            set_type = ""
            set_type = options["type"]
            _type = "{} {}".format(key, set_type)
            attrs_handled = attrs_handled + 1
            if options["type"] == "text":
                if "index" in options and "stored" in options["index"]:
                    if options["index"]["stored"]:
                        _type = _type
                    else:
                        _type = _type + " indexed"
                else:
                    _type = _type + " indexed"
            _type = _type + ","
            sql = sql + _type
        if attrs_handled == 0:
            return ""
        if sql[-1] == ',': sql = sql[:-1]
        if self.global_collection_opts and "min_infix_len" in self.global_collection_opts:
            req = "mode=raw&query=CREATE TABLE {} ({}) min_infix_len = '{}'".format(self.get_collection_name(), sql, self.global_collection_opts["min_infix_len"])
        else:
            req = "mode=raw&query=CREATE TABLE {} ({})".format(self.get_collection_name(), sql)
        print(req)
        return req


    def get_schema(self):
        new_schema = {}
        new_schema["fields"] = self.fields
        new_schema["collection"] = self.collection
        new_schema["namespace"] = self.namespace
        if self.global_collection_opts:
            new_schema["global_collection_opts"] = self.global_collection_opts
        return new_schema

    def load_schema(schema):
        return ManticoreSearchSchema(schema["namespace"], schema["collection"], schema["fields"])

    def get_collection_name(self):
        return "{}{}{}".format(self.namespace, self.namespace_separator, self.collection)

    def check_fields(self, fields):
        try:
            fields_model = SupportedFields(fields=fields)
        except Exception as e:
            raise InvalidSchema(e)

    def __init__(self, namespace, collection, fields, global_collection_opts=None):
        self.namespace_separator="_"
        self.check_fields(fields)
        self.collection = collection
        self.namespace = namespace
        self.fields = fields
        self.global_collection_opts = global_collection_opts
        