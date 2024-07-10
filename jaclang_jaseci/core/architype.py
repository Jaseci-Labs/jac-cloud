"""Core constructs for Jac Language."""

from __future__ import annotations

from dataclasses import dataclass, field
from pickle import dumps
from re import IGNORECASE, compile
from typing import (
    Callable,
    Iterable,
    Optional,
    TypeVar,
    cast,
)

from bson import ObjectId

from jaclang.compiler.constant import EdgeDir
from jaclang.core.architype import (
    Architype as _Architype,
    DSFunc,
    ObjectAnchor as _ObjectAnchor,
    ObjectType,
    Permission,
)
from jaclang.core.utils import collect_node_connections

from .collection import Collection


GENERIC_ID_REGEX = compile(r"^(g|n|e|w):([^:]*):([a-f\d]{24})$", IGNORECASE)
NODE_ID_REGEX = compile(r"^n:([^:]*):([a-f\d]{24})$", IGNORECASE)
EDGE_ID_REGEX = compile(r"^e:([^:]*):([a-f\d]{24})$", IGNORECASE)
WALKER_ID_REGEX = compile(r"^w:([^:]*):([a-f\d]{24})$", IGNORECASE)
TA = TypeVar("TA", bound="type[Architype]")


@dataclass(eq=False)
class ObjectAnchor(_ObjectAnchor):
    """Object Anchor."""

    id: ObjectId = field(default_factory=ObjectId)
    root: Optional[ObjectId] = None
    access: Permission[ObjectId] = field(default_factory=Permission[ObjectId])
    architype: Optional[Architype] = None
    connected: bool = False

    @staticmethod
    def ref(ref_id: str) -> Optional[ObjectAnchor]:
        """Return ObjectAnchor instance if ."""
        if matched := GENERIC_ID_REGEX.search(ref_id):
            cls: type = ObjectAnchor
            match ObjectType(matched.group(1)):
                case ObjectType.node:
                    cls = NodeAnchor
                case ObjectType.edge:
                    cls = EdgeAnchor
                case ObjectType.walker:
                    cls = WalkerAnchor
                case _:
                    pass
            return cls(name=matched.group(2), id=ObjectId(matched.group(3)))
        return None


@dataclass(eq=False)
class NodeAnchor(ObjectAnchor):
    """Node Anchor."""

    type: ObjectType = ObjectType.node
    architype: Optional[NodeArchitype] = None
    edges: list[EdgeAnchor] = field(default_factory=list)

    @classmethod
    def ref(cls, ref_id: str) -> Optional[NodeAnchor]:
        """Return NodeAnchor instance if existing."""
        if match := NODE_ID_REGEX.search(ref_id):
            return cls(
                name=match.group(1),
                id=ObjectId(match.group(2)),
            )
        return None

    def _save(self) -> None:
        from jaclang.core.context import ExecutionContext

        jsrc = ExecutionContext.get().datasource

        for edge in self.edges:
            edge.save()

        jsrc.set(self)

    def save(self) -> None:
        """Save Anchor."""
        if self.architype:
            if not self.connected:
                self.connected = True
                self.hash = hash(dumps(self))
                self._save()
            elif self.current_access_level > 0 and self.hash != (
                _hash := hash(dumps(self))
            ):
                self.hash = _hash
                self._save()

    def destroy(self) -> None:
        """Delete Anchor."""
        if self.architype and self.current_access_level > 1:
            from jaclang.core.context import ExecutionContext

            jsrc = ExecutionContext.get().datasource
            for edge in self.edges:
                edge.destroy()

            jsrc.remove(self)

    def sync(self, node: Optional["NodeAnchor"] = None) -> Optional[NodeArchitype]:
        """Retrieve the Architype from db and return."""
        return cast(Optional[NodeArchitype], super().sync(node))

    def connect_node(self, nd: NodeAnchor, edg: EdgeAnchor) -> None:
        """Connect a node with given edge."""
        edg.attach(self, nd)

    def get_edges(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list[EdgeArchitype]], list[EdgeArchitype]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list[EdgeArchitype]:
        """Get edges connected to this node."""
        ret_edges: list[EdgeArchitype] = []
        for anchor in self.edges:
            if (
                (architype := anchor.sync(self))
                and (source := anchor.source)
                and (target := anchor.target)
                and (not filter_func or filter_func([architype]))
            ):
                src_arch = source.sync()
                trg_arch = target.sync()

                if (
                    dir in [EdgeDir.OUT, EdgeDir.ANY]
                    and self == source
                    and trg_arch
                    and (not target_obj or trg_arch in target_obj)
                    and source.has_read_access(target)
                ):
                    ret_edges.append(architype)
                if (
                    dir in [EdgeDir.IN, EdgeDir.ANY]
                    and self == target
                    and src_arch
                    and (not target_obj or src_arch in target_obj)
                    and target.has_read_access(source)
                ):
                    ret_edges.append(architype)
        return ret_edges

    def edges_to_nodes(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list[EdgeArchitype]], list[EdgeArchitype]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list[NodeArchitype]:
        """Get set of nodes connected to this node."""
        ret_edges: list[NodeArchitype] = []
        for anchor in self.edges:
            if (
                (architype := anchor.sync(self))
                and (source := anchor.source)
                and (target := anchor.target)
                and (not filter_func or filter_func([architype]))
            ):
                src_arch = source.sync()
                trg_arch = target.sync()

                if (
                    dir in [EdgeDir.OUT, EdgeDir.ANY]
                    and self == source
                    and trg_arch
                    and (not target_obj or trg_arch in target_obj)
                    and source.has_read_access(target)
                ):
                    ret_edges.append(trg_arch)
                if (
                    dir in [EdgeDir.IN, EdgeDir.ANY]
                    and self == target
                    and src_arch
                    and (not target_obj or src_arch in target_obj)
                    and target.has_read_access(source)
                ):
                    ret_edges.append(src_arch)
        return ret_edges

    def gen_dot(self, dot_file: Optional[str] = None) -> str:
        """Generate Dot file for visualizing nodes and edges."""
        visited_nodes: set[NodeAnchor] = set()
        connections: set[tuple[NodeArchitype, NodeArchitype, str]] = set()
        unique_node_id_dict = {}

        collect_node_connections(self, visited_nodes, connections)
        dot_content = 'digraph {\nnode [style="filled", shape="ellipse", fillcolor="invis", fontcolor="black"];\n'
        for idx, i in enumerate([nodes_.architype for nodes_ in visited_nodes]):
            unique_node_id_dict[i] = (i.__class__.__name__, str(idx))
            dot_content += f'{idx} [label="{i}"];\n'
        dot_content += 'edge [color="gray", style="solid"];\n'

        for pair in list(set(connections)):
            dot_content += (
                f"{unique_node_id_dict[pair[0]][1]} -> {unique_node_id_dict[pair[1]][1]}"
                f' [label="{pair[2]}"];\n'
            )
        if dot_file:
            with open(dot_file, "w") as f:
                f.write(dot_content + "}")
        return dot_content + "}"

    def spawn_call(self, walk: WalkerAnchor) -> WalkerArchitype:
        """Invoke data spatial call."""
        return walk.spawn_call(self)

    def serialize(self) -> dict[str, object]:
        """Serialize Node Anchor."""
        return {**super().serialize(), "edges": [edge.ref_id for edge in self.edges]}


@dataclass(eq=False)
class EdgeAnchor(ObjectAnchor):
    """Edge Anchor."""

    type: ObjectType = ObjectType.edge
    architype: Optional[EdgeArchitype] = None
    source: Optional[NodeAnchor] = None
    target: Optional[NodeAnchor] = None
    is_undirected: bool = False

    @classmethod
    def ref(cls, ref_id: str) -> Optional[EdgeAnchor]:
        """Return EdgeAnchor instance if existing."""
        if match := EDGE_ID_REGEX.search(ref_id):
            return cls(
                name=match.group(1),
                id=ObjectId(match.group(2)),
            )
        return None

    def _save(self) -> None:
        from jaclang.core.context import ExecutionContext

        jsrc = ExecutionContext.get().datasource

        if source := self.source:
            source.save()

        if target := self.target:
            target.save()

        jsrc.set(self)

    def save(self) -> None:
        """Save Anchor."""
        if self.architype:
            if not self.connected:
                self.connected = True
                self.hash = hash(dumps(self))
                self._save()
            elif self.current_access_level == 1 and self.hash != (
                _hash := hash(dumps(self))
            ):
                self.hash = _hash
                self._save()

    def destroy(self) -> None:
        """Delete Anchor."""
        if self.architype and self.current_access_level == 1:
            from jaclang.core.context import ExecutionContext

            jsrc = ExecutionContext.get().datasource

            source = self.source
            target = self.target
            self.detach()

            if source:
                source.save()
            if target:
                target.save()

            jsrc.remove(self)

    def sync(self, node: Optional["NodeAnchor"] = None) -> Optional[EdgeArchitype]:
        """Retrieve the Architype from db and return."""
        return cast(Optional[EdgeArchitype], super().sync(node))

    def attach(
        self, src: NodeAnchor, trg: NodeAnchor, is_undirected: bool = False
    ) -> EdgeAnchor:
        """Attach edge to nodes."""
        self.source = src
        self.target = trg
        self.is_undirected = is_undirected
        src.edges.append(self)
        trg.edges.append(self)
        return self

    def detach(self) -> None:
        """Detach edge from nodes."""
        if source := self.source:
            source.edges.remove(self)
        if target := self.target:
            target.edges.remove(self)

        self.source = None
        self.target = None

    def spawn_call(self, walk: WalkerAnchor) -> WalkerArchitype:
        """Invoke data spatial call."""
        if target := self.target:
            return walk.spawn_call(target)
        else:
            raise ValueError("Edge has no target.")

    def serialize(self) -> dict[str, object]:
        """Serialize Node Anchor."""
        return {
            **super().serialize(),
            "source": self.source.ref_id if self.source else None,
            "target": self.target.ref_id if self.target else None,
        }


@dataclass(eq=False)
class WalkerAnchor(ObjectAnchor):
    """Walker Anchor."""

    type: ObjectType = ObjectType.walker
    architype: Optional[WalkerArchitype] = None
    path: list[ObjectAnchor] = field(default_factory=list)
    next: list[ObjectAnchor] = field(default_factory=list)
    ignores: list[ObjectAnchor] = field(default_factory=list)
    disengaged: bool = False
    persistent: bool = False  # Disabled initially but can be adjusted

    @classmethod
    def ref(cls, ref_id: str) -> Optional[WalkerAnchor]:
        """Return EdgeAnchor instance if existing."""
        if ref_id and (match := WALKER_ID_REGEX.search(ref_id)):
            return cls(
                name=match.group(1),
                id=ObjectId(match.group(2)),
            )
        return None

    def _save(self) -> None:
        from jaclang.core.context import ExecutionContext

        ExecutionContext.get().datasource.set(self)

    def save(self) -> None:
        """Save Anchor."""
        if self.architype:
            if not self.connected:
                self.connected = True
                self.hash = hash(dumps(self))
                self._save()
            elif self.current_access_level > 1 and self.hash != (
                _hash := hash(dumps(self))
            ):
                self.hash = _hash
                self._save()

    def destroy(self) -> None:
        """Delete Anchor."""
        if self.architype and self.current_access_level > 1:
            from jaclang.core.context import ExecutionContext

            ExecutionContext.get().datasource.remove(self)

    def sync(self, node: Optional["NodeAnchor"] = None) -> Optional[WalkerArchitype]:
        """Retrieve the Architype from db and return."""
        return cast(Optional[WalkerArchitype], super().sync(node))

    def visit_node(self, anchors: Iterable[NodeAnchor | EdgeAnchor]) -> bool:
        """Walker visits node."""
        before_len = len(self.next)
        for anchor in anchors:
            if anchor not in self.ignores:
                if isinstance(anchor, NodeAnchor):
                    self.next.append(anchor)
                elif isinstance(anchor, EdgeAnchor):
                    if anchor.sync() and (target := anchor.target):
                        self.next.append(target)
                    else:
                        raise ValueError("Edge has no target.")
        return len(self.next) > before_len

    def ignore_node(self, anchors: Iterable[NodeAnchor | EdgeAnchor]) -> bool:
        """Walker ignores node."""
        before_len = len(self.ignores)
        for anchor in anchors:
            if anchor not in self.ignores:
                if isinstance(anchor, NodeAnchor):
                    self.ignores.append(anchor)
                elif isinstance(anchor, EdgeAnchor):
                    if anchor.sync() and (target := anchor.target):
                        self.ignores.append(target)
                    else:
                        raise ValueError("Edge has no target.")
        return len(self.ignores) > before_len

    def disengage_now(self) -> None:
        """Disengage walker from traversal."""
        self.disengaged = True

    def spawn_call(self, nd: ObjectAnchor) -> WalkerArchitype:
        """Invoke data spatial call."""
        if walker := self.sync():
            self.path = []
            self.next = [nd]
            while len(self.next):
                if node := self.next.pop(0).sync():
                    for i in node._jac_entry_funcs_:
                        if not i.trigger or isinstance(walker, i.trigger):
                            if i.func:
                                i.func(node, walker)
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
                    for i in walker._jac_entry_funcs_:
                        if not i.trigger or isinstance(node, i.trigger):
                            if i.func:
                                i.func(walker, node)
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
                    for i in walker._jac_exit_funcs_:
                        if not i.trigger or isinstance(node, i.trigger):
                            if i.func:
                                i.func(walker, node)
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
                    for i in node._jac_exit_funcs_:
                        if not i.trigger or isinstance(walker, i.trigger):
                            if i.func:
                                i.func(node, walker)
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
            self.ignores = []
            return walker
        raise Exception(f"Invalid Reference {self.ref_id}")


class Architype(_Architype):
    """Architype Protocol."""

    class __collection__(Collection["Architype"]):  # noqa: N801
        pass

    __jac__: ObjectAnchor

    def __init__(self, __jac__: Optional[ObjectAnchor] = None) -> None:
        """Create default architype."""
        self.__jac__ = __jac__ or ObjectAnchor(architype=self)
        self.__jac__.allocate()


class NodeArchitype(Architype):
    """Node Architype Protocol."""

    __jac__: NodeAnchor

    def __init__(self, __jac__: Optional[NodeAnchor] = None) -> None:
        """Create node architype."""
        self.__jac__ = __jac__ or NodeAnchor(
            name=self.__class__.__name__, architype=self
        )
        self.__jac__.allocate()


class EdgeArchitype(Architype):
    """Edge Architype Protocol."""

    __jac__: EdgeAnchor

    def __init__(self, __jac__: Optional[EdgeAnchor] = None) -> None:
        """Create edge architype."""
        self.__jac__ = __jac__ or EdgeAnchor(
            name=self.__class__.__name__, architype=self
        )
        self.__jac__.allocate()


class WalkerArchitype(Architype):
    """Walker Architype Protocol."""

    __jac__: WalkerAnchor

    def __init__(self, __jac__: Optional[WalkerAnchor] = None) -> None:
        """Create walker architype."""
        self.__jac__ = __jac__ or WalkerAnchor(
            name=self.__class__.__name__, architype=self
        )
        self.__jac__.allocate()


class GenericEdge(EdgeArchitype):
    """Generic Root Node."""

    _jac_entry_funcs_: list[DSFunc] = []
    _jac_exit_funcs_: list[DSFunc] = []

    def __init__(self, __jac__: Optional[EdgeAnchor] = None) -> None:
        """Create walker architype."""
        self.__jac__ = __jac__ or EdgeAnchor(architype=self)
        self.__jac__.allocate()


class Root(NodeArchitype):
    """Generic Root Node."""

    _jac_entry_funcs_: list[DSFunc] = []
    _jac_exit_funcs_: list[DSFunc] = []
    reachable_nodes: list[NodeArchitype] = []
    connections: set[tuple[NodeArchitype, NodeArchitype, EdgeArchitype]] = set()

    def __init__(self, __jac__: Optional[NodeAnchor] = None) -> None:
        """Create walker architype."""
        self.__jac__ = __jac__ or NodeAnchor(architype=self)
        self.__jac__.allocate()

    def reset(self) -> None:
        """Reset the root."""
        self.reachable_nodes = []
        self.connections = set()
        self.__jac__.edges = []
