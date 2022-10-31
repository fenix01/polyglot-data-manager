

from dataclasses import field
from tokenize import String
from xmlrpc.client import Boolean
from pydantic import create_model, StrictStr, constr, StrictInt, StrictFloat, Json, BaseModel, validator
from dateutil.parser import parse
from datetime import datetime
from typing import (
    List, Dict, Optional
)
from polymanager.exceptions.schema_exception import InvalidSchema
from polymanager.schemas.graph_schema import GraphSchema 

class Index(BaseModel):
    index_type: str
    name: str
    sparse: Optional[bool]
    unique: Optional[bool]
    deduplicate: Optional[bool]
    fields: List[str]
    class Config:
        extra = "forbid"
    @validator('index_type')
    def index_in_supported_list(cls, v, values):
        types = ["persistent", "fulltext", "hash"]
        if v not in types:
            raise ValueError('{} is not a supported type'.format(v))
        return v

class Field(BaseModel):
    type_value: str = ""
    @validator('type_value')
    def type_in_supported_list(cls, v):
        if v not in ["int", "float", "text", "timestamp", "relationship", "[int]", "[float]", "[text]", "json"]:
            raise ValueError('{} is not a supported type'.format(v))
        return v

    class Config:
        fields = {'type_value': 'type'}
        extra = "forbid"

class SupportedFields(BaseModel):
    fields_value: Dict[str, Field]
    class Config:
        fields = {'fields_value': 'fields'}

class GlobalOptions(BaseModel):
    indexes: List[Index]
    edge_collection: Boolean

class ArangodbSchema(GraphSchema):

    def generate_schema(self):
        pass

    def get_edge_schema(self, edge):
        new_edge = {}
        for key, value in edge.items():
            field_options = self.fields[key]
            new_value = self.parse_field(field_options["type"], value)
            new_edge[key] = new_value
        return new_edge

    def get_node_schema(self, node):
        new_node = {}
        for key, value in node.items():
            field_options = self.fields[key]
            new_value = self.parse_field(field_options["type"], value)
            new_node[key] = new_value
        return new_node

    def get_relationship_fields(self):
        fields = list()
        for key, options in self.get_fields().items():
            fields.append(key)
        return fields

    def parse_field(self, field_type, field_value):
        if field_type == "text":
            return field_value
        elif field_type == "int":
            return field_value
        elif field_type == "float":
            return field_value
        elif field_type == "timestamp":
            return parse(field_value).isoformat()
        elif field_type == "relationship":
            return field_value
        else:
            return field_value

    def check_relationship_schema(self, node):
        attrs_scheme = dict()
        for key, options in self.fields.items():
            if options["type"] == "relationship":
                attrs_scheme[key] = (StrictStr, ...)
            elif options["type"] == "text":
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
            elif options["type"] == "json":
                attrs_scheme[key] = (Json, ...)
        node_model = create_model('DynamicNodeModel', **attrs_scheme)
        node_model.__config__.extra = 'forbid'
        node_model.__config__.validate_all = True
        try:
            if len(node.keys()) == 0:
                raise Exception()
            node_model(**node)
        except Exception as e:
            raise InvalidSchema(e)

    def check_node_schema(self, node):
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
            elif options["type"] == "json":
                attrs_scheme[key] = (Json, ...)
        node_model = create_model('DynamicNodeModel', **attrs_scheme)
        node_model.__config__.extra = 'forbid'
        node_model.__config__.validate_all = True
        try:
            if len(node.keys()) == 0:
                raise Exception()
            node_model(**node)
        except Exception as e:
            raise InvalidSchema(e)

    def get_fields(self):
        return self.fields

    def get_field(self, field):
        return field

    def get_schema(self):
        new_schema = {}
        new_schema["fields"] = self.fields
        new_schema["collection"] = self.collection
        new_schema["namespace"] = self.namespace
        if self.global_collection_opts:
            new_schema["global_collection_opts"] = self.global_collection_opts
        return new_schema

    def load_schema(schema):
        if "global_collection_opts" in schema:
            return ArangodbSchema(schema["namespace"], schema["collection"], schema["fields"], global_collection_opts=schema["global_collection_opts"])
        else:
            return ArangodbSchema(schema["namespace"], schema["collection"], schema["fields"])

    def get_collection_name(self):
            return self.collection

    def get_namespace(self):
        return self.namespace

    def check_fields(self, fields):
        try:
            data = {
                "fields": fields
            }
            SupportedFields(**data)
            fields_model = SupportedFields(fields=fields)
        except Exception as e:
            raise InvalidSchema(e)

    def check_global_options(self, global_collection_opts):
        try:
            GlobalOptions(**global_collection_opts)
            #we need to check the value of edge_collection.
            #if it is True then it a special arangodb collection. It is mandatory to have these fields (_from and _to )
            if global_collection_opts["edge_collection"]:
                if "from" not in self.fields:
                    raise InvalidSchema("missing from field requires for edge collection.")
                else:
                    if self.fields["from"]["type"] != "relationship":
                        raise InvalidSchema("from field must be relationship.")
                _to = self.fields["to"]
                if "to" not in self.fields:
                    raise InvalidSchema("missing tto field requires for edge collection.")
                else:
                    if self.fields["to"]["type"] != "relationship":
                        raise InvalidSchema("to field must be relationship.")
            else:
                #we must verify that there is not any  fields of type relationship if it is not a edge collection.
                for key, options in self.fields.items():
                    if options["type"] == "relationship":
                        raise InvalidSchema("relationship field is not allowed.")
        except Exception as e:
            raise InvalidSchema(e)

    def get_global_collection_opts(self):
        return self.global_collection_opts

    def __init__(self, namespace, collection, fields, global_collection_opts=None):
        self.check_fields(fields)
        self.collection = collection
        self.namespace = namespace
        self.fields = fields
        self.global_collection_opts = global_collection_opts
        if self.global_collection_opts:
            self.check_global_options(global_collection_opts)