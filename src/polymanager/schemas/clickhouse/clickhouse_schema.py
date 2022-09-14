

from pydantic import create_model, StrictStr, StrictInt, StrictFloat, validator
from dateutil.parser import parse
from datetime import datetime
from typing import (
    List, Dict
)
from pydantic import BaseModel
from polymanager.exceptions.schema_exception import InvalidSchema
from polymanager.schemas.document_schema import DocumentSchema

class Field(BaseModel):
    type_value: str = ""
    @validator('type_value')
    def type_in_supported_list(cls, v):
        if v not in ["int", "float", "text", "timestamp", "[int]", "[float]", "[text]"]:
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

class GlobalOptions(BaseModel):
    order_by: List[str]

class ClickhouseSchema(DocumentSchema):

    def get_fields(self):
        if self.include_namespace:
            new_fields = {}
            for key, options in self.fields.items():
                options = self.fields[key]
                new_key = "{}{}{}".format(self.namespace, self.namespace_separator, key)
                if key == "id":
                    new_fields["id"] = options
                else:
                    new_fields[new_key] = options
            return new_fields
        else:
            return self.fields

    def get_field(self, field):
        if self.include_namespace:
            return "{}{}{}".format(self.namespace, self.namespace_separator, field)

    def get_document_schema(self, document):
        new_document = {}
        for key, value in document.items():
            field_options = self.fields[key]
            new_value = self.parse_field(field_options["type"], value)
            if key == "id":
                new_document["id"] = new_value
            else:
                new_document[key] = new_value
        return new_document

    def parse_field(self, field_type, field_value):
        if field_type == "text":
            return field_value
        elif field_type == "int":
            return field_value
        elif field_type == "float":
            return field_value
        elif field_type == "timestamp":
            return parse(field_value).strftime("%Y-%m-%d %H:%M:%S")
        else:
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
            elif options["type"] == "[int]":
                attrs_scheme[key] = (List[StrictInt], ...)
            elif options["type"] == "[text]":
                attrs_scheme[key] = (List[StrictStr], ...)
            elif options["type"] == "[float]":
                attrs_scheme[key] = (List[StrictFloat], ...)
        document_model = create_model('DynamicNodeModel', **attrs_scheme)
        document_model.__config__.extra = 'forbid'
        document_model.__config__.validate_all = True
        try:
            document_model(**document)
        except Exception as e:
            raise InvalidSchema(e)

    def generate_schema(self):
        sql = ""
        attrs_handled = 0
        for index, key in enumerate(self.fields):
            options = self.fields[key]
            _type = ""
            set_type = ""
            if options["type"] == "text":
                set_type = "String"
            elif options["type"] == "timestamp":
                set_type = "Datetime"
            elif options["type"] == "int":
                set_type = "Int64"
            elif options["type"] == "float":
                set_type = "Float32"
            elif options["type"] == "[int]":
                set_type = "Array(Int64)"
            elif options["type"] == "[float]":
                set_type = "Array(Float32)"
            elif options["type"] == "[text]":
                set_type = "Array(String)"
            _type = "{} {}".format(key, set_type)
            attrs_handled = attrs_handled + 1
            _type = _type + ","
            sql = sql + _type
        if attrs_handled == 0:
            return ""
        if sql[-1] == ',': sql = sql[:-1]
        req = "query=CREATE TABLE {} ({}) ENGINE = MergeTree() ORDER BY ({})".format(
            self.get_collection_name(),
            sql,
            ",".join(self.global_collection_opts["order_by"])
        )
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
            return ClickhouseSchema(schema["namespace"], schema["collection"], schema["fields"], schema["global_collection_opts"])

    def get_collection_name(self):
            return self.namespace+"."+self.collection
        
    def get_namespace(self):
            return self.namespace

    def check_fields(self, fields):
        try:
            fields_model = SupportedFields(fields=fields)
        except Exception as e:
            raise InvalidSchema(e)

    def check_global_options(self, global_collection_opts):
        try:
            GlobalOptions(**global_collection_opts)
        except Exception as e:
            raise InvalidSchema(e)


    def __init__(self, namespace, collection, fields, global_collection_opts):
        self.namespace_separator="_"
        self.include_namespace = True
        self.check_fields(fields)
        self.check_global_options(global_collection_opts)
        self.collection = collection
        self.namespace = namespace
        self.fields = fields
        self.global_collection_opts = global_collection_opts
        