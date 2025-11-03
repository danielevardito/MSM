from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from enum import Enum
from typing import List
from datetime import datetime
from digraph import ValidatedArangoGraph
import os

class Category(str, Enum):
    """Metadata category types (not in why3 specs)."""
    CONCEPT = "concept"
    LANGUAGE = "language"

class RelationType(str, Enum):
    METADATA_PARENT = "metadata_parent" # metadata  ->  metadata
    HAS_METADATA    = "has_metadata"    # snippet   ->  metadata

class Metadata(BaseModel):
    name: str = Field(pattern=r'^[a-z0-9_]+$')
    category: Category

class Snippet(BaseModel):
    name: str
    content: str
    extension: str = Field(pattern=r'^[a-z]+$', max_length=10)
    created_at: datetime = Field(default_factory=datetime.now)

    @field_validator('extension', mode='before')
    @classmethod
    def force_extension_lowercase(cls, v: str) -> str:
        """Forza l'estensione in minuscolo prima di validarla"""
        if isinstance(v, str):
            return v.lower()
        return v

    @model_validator(mode='after')
    def ensure_name_has_correct_extension(self) -> 'Snippet':
        base_name, _ = os.path.splitext(self.name)
        return f"{base_name}.{self.extension}"

class MSMDiGraph(ValidatedArangoGraph):
    """The specific graph to use as database."""

    def __init__(self, adb_graph):
        super().__init__(adb_graph, RelationType)

    def _node_data_with_key(self, key_field: str, node_key: str) -> dict:
        node = dict(self.G.nodes[node_key])
        node[key_field] = node_key
        return node

    def _parse_metadata(self, data_string: str) -> Metadata:
        name: str
        category: Category
        try:
            name, category = data_string.split("-", 1)
        except ValueError:
            raise ValueError(
                f"String not valid: {data_string}"
                f"Requested format: name-category"
            )

        metadata = Metadata(name=name, category=category)
        return metadata
            
    def _format_metdata(self, metadata: Metadata) -> str:
        return f"{metadata.name}-{metadata.category}"

    def is_metadata(self, node_key: str) -> bool:
        if not self.memv(node_key):
            return False
        try:
            self._parse_metadata(node_key)
            return True
        except:
            return False

    def is_snippet(self, node_key: str) -> bool:
        if not self.memv(node_key):
            return False

        try:
            Snippet.parse_obj(self._node_data_with_key("name", node_key))
            return True
        except ValidationError:
            return False

    def insert_freemetadata(self, metadata: Metadata) -> str:
        """
        Inserts a metadata without parent. NodeKey is name-category because metadata with same 
        name but with different category could exist.
        """
        key = self._format_metdata(metadata) 

        if self.memv(key):
            raise KeyError("Metadata already exists")
        self.insertv(metadata, key)
        return key

    def insert_metadata(self, metadata: Metadata, parent: Metadata) -> str:
        new_metadata_key = self._format_metdata(metadata)
        parent_key = self._format_metdata(parent)

        if not (self.is_metadata(parent_key)):
            raise ValueError("Parent metadata doesn't exist")

        if metadata.category != parent.category:
            raise ValueError(f"Parent category ({parent.category}) and child category ({metadata.category}) must match")

        if self.memv(new_metadata_key):
            raise ValueError("Metadata already exists")

        self.insertv(metadata, new_metadata_key, None)
        self.inserte(parent_key, new_metadata_key, RelationType.METADATA_PARENT)

    def insert_metadata_list_for_snippet(self, snippet_key: str, metadata_list: List[str]):
        if not self.is_snippet(snippet_key):
            raise ValueError(f"Snippet {snippet_key} isn't in db")

        match metadata_list:
            case []: return
            case [m, *l]:
                if not self.is_metadata(m): # Precondition in why3 (forall m)
                    raise ValueError(f"Metadata {metadata_list} isn't in db")
                self.inserte(snippet_key, m, RelationType.HAS_METADATA)
                self.insert_metadata_list_for_snippet(snippet_key, l)

    def insert_snippet(self, snippet: Snippet, metadata_list: List[str]):
        if self.memv(snippet.name):
            raise ValueError(f"Snippet {snippet.name} already exists")
        
        self.insertv(snippet, snippet.name, "name")
        self.insert_metadata_list_for_snippet(snippet.name, metadata_list)
         
    # TODO: categoria in insert_snippet


        
