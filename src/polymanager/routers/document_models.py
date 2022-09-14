from pydantic import BaseModel, conlist
from typing import (
    List, Optional
)
class UpdateDocument(BaseModel):
    collection: str
    namespace: str
    document: dict = {}

class DelDocument(BaseModel):
    collection: str
    namespace: str
    document_id: int

class DelDocuments(BaseModel):
    collection: str
    namespace: str
    documents_id: list

class Document(BaseModel):
    collection: str
    namespace: str
    document: dict = {}

class Documents(BaseModel):
    collection: str
    namespace: str
    documents: conlist(dict, min_items=1)

# class ClickhouseGlobalOptions(BaseModel):
#     order_by: List[str]

class Collection(BaseModel):
    collection: str
    namespace: str
    field_value: dict = {}
    global_options: dict = {}
    class Config:
        fields = {'field_value': 'fields'}

# class ManticoreseearchGlobalOptions(BaseModel):
#     min_infix_len: int

# class ManticoreSearchCollection(BaseModel):
#     collection: str
#     namespace: str
#     field_value: dict = {}
#     global_options: Optional[ManticoreseearchGlobalOptions]
#     class Config:
#         fields = {'field_value': 'fields'}

class CollectionDel(BaseModel):
    collection: str
    empty: bool
    namespace: str