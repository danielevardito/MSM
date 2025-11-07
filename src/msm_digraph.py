from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from enum import Enum
from typing import List, Tuple, Set
from datetime import datetime
from digraph import ValidatedArangoGraph
import os
import networkx as nx

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
        """Force extension to lowercase before validation"""
        if isinstance(v, str):
            return v.lower()
        return v

    @model_validator(mode='after')
    def ensure_name_has_correct_extension(self) -> 'Snippet':
        """Ensure name has the correct extension"""
        base_name, _ = os.path.splitext(self.name)
        self.name = f"{base_name}.{self.extension}"
        return self

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
                f"String not valid: {data_string}. "
                f"Requested format: name-category"
            )

        metadata = Metadata(name=name, category=category)
        return metadata
            
    def _format_metdata(self, metadata: Metadata) -> str:
        return f"{metadata.name}-{metadata.category.value}"

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
            Snippet.model_validate(self._node_data_with_key("name", node_key))
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
        
        # Convert metadata to dict for storage
        data = metadata.model_dump()
        self.insertv(data, key)
        return key

    def insert_metadata(self, metadata: Metadata, parent: Metadata, category: Category) -> str:

        if metadata.category != category or parent.category != category:
            raise ValueError(f"Category mismatch: child:{metadata.category}, parent:{parent.category} category to insert:{category}")

        new_metadata_key = self._format_metdata(metadata)
        parent_key = self._format_metdata(parent)

        if not (self.is_metadata(parent_key)):
            raise ValueError("Parent metadata doesn't exist")

        if metadata.category != parent.category:
            raise ValueError(f"Parent category ({parent.category}) and child category ({metadata.category}) must match")

        if self.memv(new_metadata_key):
            raise ValueError("Metadata already exists")

        data = metadata.model_dump()
        self.insertv(data, new_metadata_key)
        self.inserte(parent_key, new_metadata_key, RelationType.METADATA_PARENT)
        return new_metadata_key

    def _metadata_present_same_cat(self, metadata_list: List[Metadata], category: Category):
        match metadata_list:
            case []: 
                return 
            case [m, *l]:
                if not self.is_metadata(self._format_metdata(m)):
                    raise ValueError(f"Metadata {m} isn't in db")
                if m.category != category:
                    raise ValueError(f"Metadata category: {m} doesn't match requested category:{category}")
                self._metadata_present_same_cat(l, category)
                

    def _insert_metadata_list_for_snippet(self, snippet_key: str, metadata_list: List[Metadata], category: Category):
        if not self.is_snippet(snippet_key): # precondition
            raise ValueError(f"Snippet {snippet_key} isn't in db")

        self._metadata_present_same_cat(metadata_list, category) # precondition + category control

        match metadata_list:
            case []: 
                return
            case [m, *l]:
                self.inserte(snippet_key, self._format_metdata(m), RelationType.HAS_METADATA)
                self._insert_metadata_list_for_snippet(snippet_key, l, category)

    def insert_snippet(self, snippet: Snippet, metadata_list: List[Metadata], category: Category):
        if self.memv(snippet.name): # precondition
            raise ValueError(f"Snippet {snippet.name} already exists")
        if len(metadata_list) < 1:
            raise ValueError("There should be at least one metadata name")
        
        data = snippet.model_dump(mode='json')
        self.insertv(data, snippet.name)
        self._insert_metadata_list_for_snippet(snippet.name, metadata_list, category)

    def _get_all_metadata_from_snippet(self, snippet_name: str) -> List[Metadata]:
        metadata_keys = self.successors(snippet_name)
        return [self._parse_metadata(key) for key in metadata_keys]

    def get_snippet(self, snippet_name: str) -> Tuple[Snippet, List[Metadata]]:
        if not self.is_snippet(snippet_name):
            raise ValueError(f"Snippet {snippet_name} does not exist")

        snippet = self.get_node(snippet_name)
        snippet_obj = Snippet.model_validate(snippet)
        metadata_list = self._get_all_metadata_from_snippet(snippet_name)
        return snippet_obj, metadata_list

    def _get_snippets_with_metadata_from_list(self, l: List[str]) -> List[Tuple[Snippet, List[Metadata]]]:
        match l:
            case []: return []
            case [s, *r]: return [self.get_snippet(s)] + self._get_snippets_with_metadata_from_list(r)

    def _filter_snippets_from_vertices (self, l: List[str]) -> List[str]:
        match l:
            case []: return []
            case [x, *r]:
                tail = self._filter_snippets_from_vertices(r)
                rec = [x] + tail if self.is_snippet(x) else tail 
                return rec

    def get_all_snippets(self) -> List[Tuple[Snippet, List[Metadata]]]:
        all_snippets = self._filter_snippets_from_vertices (self.vertices_list())
        all_snippets_with_metadata = self._get_snippets_with_metadata_from_list(all_snippets)
        return all_snippets_with_metadata

    def update_snippet_content(self, snippet_name: str, content: str):
        snippet, _ = self.get_snippet(snippet_name)
        snippet.content = content
        updated_data = snippet.model_dump(mode='json')
        self.updatev(snippet_name, updated_data)

    def _get_snippets_from_metadata(self, m: Metadata) -> Set[str]:
        try:
            pred = self.predecessors(self._format_metdata(m))
            snippets = self._filter_snippets_from_vertices(pred)
            return set(snippets)  
        except:
            return set()

    def _get_snippets_union_set(self, metadata_list: List[Metadata]) -> Set[str]:
        match metadata_list:
            case []: 
                return set()
            case [m, *r]:
                return self._get_snippets_from_metadata(m).union(self._get_snippets_union_set(r))
    def get_snippets_union(self, metadata_list: List[Metadata]) -> List[Tuple[Snippet, List[Metadata]]]:
        snippet_keys = list(self._get_snippets_union_set(metadata_list))
        return self._get_snippets_with_metadata_from_list(snippet_keys)

    def _get_snippets_intersection_set(self, metadata_list: List[Metadata]) -> Set[str]:
        match metadata_list:
            case []:
                return set()
            case [m]: return self._get_snippets_from_metadata(m)
            case [m, *r]:
                rest_set = self._get_snippets_intersection_set(r)
                return self._get_snippets_from_metadata(m).intersection(rest_set)
    def get_snippets_intersection(self, metadata_list: List[Metadata]) -> List[Tuple[Snippet, List[Metadata]]]:
        snippet_keys = list(self._get_snippets_intersection_set(metadata_list))
        return self._get_snippets_with_metadata_from_list(snippet_keys)

    def delete_snippet(self, snippet_name: str):
        if not self.is_snippet(snippet_name):
            raise ValueError(f"Snippet {snippet_name} not found")
        self.deletev(snippet_name)

    def _snippet_metadata_outdegree(self, s_key: str) -> int:
        if not self.is_snippet(s_key):
            raise ValueError(f"Snippet {s_key} not found")
        
        return len(self.successors(s_key))

    def _only_metadata_of_snippet(self, m_key: str, s_key: str) -> bool:
        if self.is_metadata(m_key) and self.is_snippet(s_key):
            return self._snippet_metadata_outdegree(s_key) == 1 and self.meme(s_key, m_key)

    def _delete_snippets_with_only_m(self, m_key: str, s_keys: List[str]):
        match s_keys:
            case []: return
            case [s_key, *r]:
                if self._only_metadata_of_snippet(m_key, s_key):
                    self.delete_snippet(s_key)
                    self._delete_snippets_with_only_m(m_key, r)
                else: 
                    self._delete_snippets_with_only_m(m_key, r)

    def delete_metadata(self, m: Metadata):
        m_key = self._format_metdata(m)
        if not self.is_metadata(m_key):
            raise ValueError(f"Metadata {m_key} doesn't exist")
        
        snippets = self._get_snippets_from_metadata(m)
        self._delete_snippets_with_only_m(m_key, list(snippets))
        self.deletev(m_key)

    def _metadata_indegree(self, node_key: str) -> int:
        if not self.is_metadata(node_key):
            raise ValueError(f"Node {node_key} is not metadata")

        count = 0
        for pred in self.predecessors(node_key):
            if self.is_metadata(pred):
                count += 1
        return count

    def _filter_metadata_parents(self, m_list: List[str]) -> List[str]:
        match m_list:
            case []: return []
            case [m, *r]:
                if self.is_metadata(m) and self._metadata_indegree(m) == 0:
                    return [m] + self._filter_metadata_parents(r)
                else:
                    return self._filter_metadata_parents(r)

    def get_all_roots(self) -> List[str]:
        return self._filter_metadata_parents(self.vertices_list())

    def _filter_metadata_from_list(self, l: List[str]) -> List[str]:
        match l:
            case []: return []
            case [x, *r]:
                if self.is_metadata(x):
                    return [x] + self._filter_metadata_from_list(r)
                else:
                    return self._filter_metadata_from_list(r)

    def _collect_reachable_metadata(self, worklist: List[str], visited: Set[str]) -> Set[str]:
        match worklist:
            case []:
                return visited
            case [current, *rest]:
                succ_list = self.successors(current)
                metadata_succ = self._filter_metadata_from_list(succ_list)

                visited_new = visited.copy()
                for m in metadata_succ:
                    visited_new.add(m)

                worklist_new = rest.copy()
                for m in metadata_succ:
                    if m not in visited:
                        worklist_new.append(m)
                
                return self._collect_reachable_metadata(worklist_new, visited_new)

    def get_metadata_tree(self, metadata_key: str) -> nx.DiGraph:
        if not self.is_metadata(metadata_key):
            raise ValueError(f"Node {metadata_key} is not metadata")

        reachable = self._collect_reachable_metadata([metadata_key], {metadata_key})
        tree = nx.DiGraph()

        for node_key in reachable:
            node_data = self.get_node(node_key)
            tree.add_node(node_key, **node_data)

        for src_key in reachable:
            for dst_key in self.successors(src_key):
                if dst_key in reachable:
                    edge_label = self.getelabel(src_key, dst_key)
                    tree.add_edge(src_key, dst_key, label=edge_label)

        return tree