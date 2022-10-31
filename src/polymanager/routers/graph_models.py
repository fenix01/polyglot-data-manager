from pydantic import BaseModel, conlist
from typing import (
    List
)
from typing import Optional

class DelEdges(BaseModel):
    collection: str
    namespace: str
    edges_id: list = []

class UpdateEdge(BaseModel):
    collection: str
    namespace: str
    reset: Optional[bool] = False
    edge: dict = {}

class UpdateRelationship(BaseModel):
    collection: str
    namespace: str
    node: dict = {}
    reset: Optional[bool] = False
    node_id: str

class UpdateNode(BaseModel):
    collection: str
    namespace: str
    node: dict = {}
    node_id: str

class DelNode(BaseModel):
    collection: str
    namespace: str
    node_id: str

class DelNodes(BaseModel):
    collection: str
    namespace: str
    nodes_id: list = []

class Node(BaseModel):
    collection: str
    namespace: str
    node: dict = {}

class Nodes(BaseModel):
    collection: str
    namespace: str
    nodes: list = []

class Collection(BaseModel):
    collection: str
    namespace: str
    field_value: dict = {}
    global_options: dict = {}
    class Config:
        fields = {'field_value': 'fields'}

class CollectionDel(BaseModel):
    collection: str
    empty: bool
    namespace: str
