import nx_arangodb as nxadb
from pydantic import BaseModel
from enum import Enum
from typing import TypeVar, Dict, Any, Optional, Tuple, Type, List, Any as AnyType

M = TypeVar("M", bound=BaseModel)

class ValidatedArangoGraph:
    """Wrapper for managing directed graphs conforming to Why3 DiGraph theory."""
    
    def __init__(self, adb_graph: nxadb.Graph, relation_enum: Type[Enum]):
        if not isinstance(adb_graph, nxadb.Graph):
            raise TypeError("adb_graph must be an instance of nx_arangodb.Graph")
        if not issubclass(relation_enum, Enum):
            raise TypeError("relation_enum must be an Enum class")
        if not adb_graph.is_directed():
            raise TypeError("ValidatedArangoGraph requires a directed graph (is_directed=True)")

        self.G: nxadb.Graph = adb_graph
        self.relation_enum: Type[Enum] = relation_enum

    def _normalize_node_id(self, node_id: AnyType) -> str:
        """Extract key from 'collection/key' or return stringified value."""
        return str(node_id).rsplit('/', 1)[-1]

    def _edge_tuple_from_any(self, e: AnyType) -> Tuple[str, str]:
        """Convert various edge representations to (src_key, dst_key) tuple."""
        if isinstance(e, (tuple, list)) and len(e) >= 2:
            return self._normalize_node_id(e[0]), self._normalize_node_id(e[1])
        if isinstance(e, dict) and 'src' in e and 'dst' in e:
            return self._normalize_node_id(e['src']), self._normalize_node_id(e['dst'])
        s = str(e)
        if '/' in s:
            parts = s.split('/')
            if len(parts) >= 2:
                return parts[-2], parts[-1]
        raise TypeError("Unrecognized edge format. Use (src, dst) or {'src':..,'dst':..}.")

    def _validate_relation_type(self, relation_type: Enum) -> str:
        """Validate relation type is a member of the graph's relation enum."""
        if not isinstance(relation_type, self.relation_enum):
            raise TypeError(
                f"Invalid relation type: '{relation_type}'. "
                f"Must be a member of {self.relation_enum.__name__} Enum."
            )
        return str(relation_type.value)

    def memv(self, node_key: str) -> bool:
        """Check if vertex exists in graph."""
        return node_key in self.G

    def meme(self, source_key: str, target_key: str) -> bool:
        """Check if edge exists."""
        return self.G.has_edge(source_key, target_key)

    def src(self, e: AnyType) -> str:
        """Get source vertex of edge."""
        u, _ = self._edge_tuple_from_any(e)
        return u

    def dst(self, e: AnyType) -> str:
        """Get destination vertex of edge."""
        _, v = self._edge_tuple_from_any(e)
        return v

    def cardinalv(self) -> int:
        """Get number of vertices."""
        return int(self.G.number_of_nodes())

    def cardinale(self) -> int:
        """Get number of edges."""
        return int(self.G.number_of_edges())

    def vertices_list(self) -> List[str]:
        """Return list of all vertex keys."""
        return [self._normalize_node_id(n) for n in self.G.nodes()]

    def edges_list(self) -> List[Tuple[str, str]]:
        """Return list of all edges as (src_key, dst_key) tuples."""
        res: List[Tuple[str, str]] = []
        for edge in self.G.edges():
            u, v = edge[0], edge[1]
            res.append((self._normalize_node_id(u), self._normalize_node_id(v)))
        return res

    def indegree(self, node_key: str) -> int:
        """Get in-degree of vertex."""
        if node_key not in self.G:
            raise KeyError(f"Vertex '{node_key}' not found.")
        return int(self.G.in_degree(node_key))

    def outdegree(self, node_key: str) -> int:
        """Get out-degree of vertex."""
        if node_key not in self.G:
            raise KeyError(f"Vertex '{node_key}' not found.")
        return int(self.G.out_degree(node_key))

    def degree(self, node_key: str) -> int:
        """Get total degree of vertex (number of incident edges)."""
        if node_key not in self.G:
            raise KeyError(f"Vertex '{node_key}' not found.")
        return len(self.edgesv_set(node_key))

    def edgesv_list(self, node_key: str) -> List[Tuple[str, str]]:
        """Get list of edges incident to vertex."""
        return list(self.edgesv_set(node_key))

    def insertv(self, node_data: Dict[str, Any], node_key: str) -> str:
        """Insert a vertex into the graph."""
        if node_key in self.G:
            raise KeyError(f"Vertex with key '{node_key}' already exists.")
        
        data = node_data if node_data is not None else {}

        self.G.add_node(node_key, **data)
        return node_key

    def deletev(self, node_key: str):
        """Delete vertex and all its incident edges from graph."""
        if node_key not in self.G:
            raise KeyError("Node key is not in G")
        self.G.remove_node(node_key)

    def inserte(self, source_key: str, target_key: str, relation_type: Enum) -> Tuple[str, str]:
        """Insert edge with validated relation type."""
        label = self._validate_relation_type(relation_type)

        if source_key not in self.G:
            raise KeyError(f"Source vertex ('{source_key}') not found.")
        if target_key not in self.G:
            raise KeyError(f"Target vertex ('{target_key}') not found.")
        if self.G.has_edge(source_key, target_key):
            raise KeyError(f"Edge ('{source_key}' -> '{target_key}') already exists.")

        self.G.add_edge(source_key, target_key, label=label)
        return source_key, target_key

    def deletee(self, source_key: str, target_key: str):
        """Delete edge from graph."""
        if not self.G.has_edge(source_key, target_key):
            raise KeyError(f"Attempted to delete non-existent edge ('{source_key}' -> '{target_key}').")
        self.G.remove_edge(source_key, target_key)

    def successors(self, node_key: str) -> List[str]:
        """Get list of successor vertices."""
        return [self._normalize_node_id(n) for n in self.G.successors(node_key)] 

    def predecessors(self, node_key: str) -> List[str]:
        """Get list of predecessor vertices."""
        return [self._normalize_node_id(n) for n in self.G.predecessors(node_key)] 

    def getelabel(self, source_key: str, target_key: str) -> Optional[str]:
        """Get edge label (relation type) - utility function for Python."""
        if self.G.has_edge(source_key, target_key):
            return self.G.edges[(source_key, target_key)].get('label')
        return None

    def get_node(self, key: str) -> Dict[str, Any]:
        return self.G.nodes[key]