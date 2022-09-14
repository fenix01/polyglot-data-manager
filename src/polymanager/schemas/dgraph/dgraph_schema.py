

from pydantic import create_model, StrictStr, constr, StrictInt, StrictFloat, BaseModel, validator
from dateutil.parser import parse
from datetime import datetime
from typing import (
    List, Dict, Optional
)
from polymanager.exceptions.schema_exception import InvalidSchema
from polymanager.schemas.graph_schema import GraphSchema 
class Index(BaseModel):
    tokenizer: str
    class Config:
        extra = "forbid"
    @validator('tokenizer')
    def tokenizer_in_supported_list(cls, v, values):
        # mappings = {
        #     "string": ["hash", "exact", "term", "trigram"],
        #     "datetime": ["year", "month", "day", "hour"],
        #     "float": ["float"],
        #     "int": ["int"]
        # }
        types = ["hash", "exact", "term", "trigram", "year", "month", "day", "hour", "int", "float"]
        if v not in types:
            raise ValueError('{} is not a supported type'.format(v))
        return v

class Field(BaseModel):
    type_value: str = ""
    index: Optional[Index] = None
    @validator('type_value')
    def type_in_supported_list(cls, v):
        if v not in ["int", "float", "text", "timestamp", "relationship"]:
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
    pass

class DGraphSchema(GraphSchema):

    def check_tokenizer(self, _type, options):
        mappings = {
            "string": ["hash", "exact", "term", "trigram"],
            "datetime": ["year", "month", "day", "hour"],
            "float": ["float"],
            "int": ["int"]
        }
        if "tokenizer" not in options:
            raise Exception("you should defined a supported tokenizer for the attribute ({})".format(",".join(["hash", "exact", "term", "trigram", "year", "month", "day", "hour", "int", "float"])))
        elif options["tokenizer"] not in mappings[_type]:
            raise Exception("you should defined a valid tokenizer for the attribute ({})".format(",".join(mappings[_type])))


    def generate_schema(self):
        req = ""
        predicates = ""
        predicated_handled = 0
        for key, options in self.get_fields().items():
            tokenizer = ""
            set_type = ""
            if options["type"] == "text":
                set_type = "string"
            elif options["type"] == "timestamp":
                set_type = "datetime"
            elif options["type"] == "int":
                set_type = "int"
            elif options["type"] == "float":
                set_type = "float"
            elif options["type"] == "relationship":
                set_type = "[uid]"
            if "index" in options:
                try:
                    self.check_tokenizer(set_type, options["index"])
                    tokenizer = "@index({})".format(options["index"]["tokenizer"])
                except Exception as e:
                    raise e
            if tokenizer:
                 _type = "{}: {} {} .\n".format(key, set_type, tokenizer)
            else:
                 _type = "{}: {} .\n".format(key, set_type)
            predicates = predicates + key + "\n"
            predicated_handled = predicated_handled + 1
            req = req + _type
        if predicated_handled == 0:
            raise Exception("this schema does not contain any attribute to treat")
        else:
            req = req + '''type <{}> {{\n{}}}'''.format(self.get_collection_name(), predicates)
            return req

    def get_namespace(self):
            return self.namespace

    def get_node_schema(self, node):
        new_node = {}
        for key, value in node.items():
            field_options = self.fields[key]
            new_key = "{}{}{}".format(self.namespace, self.namespace_separator, key)
            new_value = self.parse_field(field_options["type"], value)
            new_node[new_key] = new_value
        return new_node

    def get_relationship_fields(self):
        fields = list()
        for key, options in self.get_fields().items():
            if options["type"] == "relationship":
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

    def check_relationship_schema(self, node):
        attrs_scheme = dict()
        for key, options in self.fields.items():
            if options["type"] == "relationship":
                attrs_scheme[key] = (List[constr(regex="0x[0-9a-fA-F]+")], ...)
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
        new_fields = {}
        for key, options in self.fields.items():
            options = self.fields[key]
            new_key = "{}{}{}".format(self.namespace, self.namespace_separator, key)
            new_fields[new_key] = options
        return new_fields

    def get_field(self, field):
        return "{}{}{}".format(self.namespace, self.namespace_separator, field)

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
            return DGraphSchema(schema["namespace"], schema["collection"], schema["fields"], global_collection_opts=schema["global_collection_opts"])
        else:
            return DGraphSchema(schema["namespace"], schema["collection"], schema["fields"])

    def get_collection_name(self):
            return "{}{}{}".format(self.namespace, self.namespace_separator, self.collection)

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
        except Exception as e:
            raise InvalidSchema(e)

    def __init__(self, namespace, collection, fields, global_collection_opts=None):
        self.namespace_separator="."
        self.check_fields(fields)
        self.collection = collection
        self.namespace = namespace
        self.fields = fields
        self.global_collection_opts = global_collection_opts
        if self.global_collection_opts:
            self.check_global_options(global_collection_opts)