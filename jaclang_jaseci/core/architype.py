"""Core constructs for Jac Language."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from inspect import iscoroutine
from pickle import dumps
from re import IGNORECASE, compile
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    TypeVar,
    cast,
)

from bson import ObjectId

from jaclang.compiler.constant import EdgeDir
from jaclang.core.architype import (
    Anchor as _Anchor,
    AnchorType,
    Architype as _Architype,
    DSFunc,
    Permission,
)
from jaclang.core.utils import collect_node_connections

from motor.motor_asyncio import AsyncIOMotorClientSession

from pymongo import ASCENDING

from ..jaseci.datasources import Collection as BaseCollection
from ..jaseci.utils import logger


GENERIC_ID_REGEX = compile(r"^(g|n|e|w):([^:]*):([a-f\d]{24})$", IGNORECASE)
NODE_ID_REGEX = compile(r"^n:([^:]*):([a-f\d]{24})$", IGNORECASE)
EDGE_ID_REGEX = compile(r"^e:([^:]*):([a-f\d]{24})$", IGNORECASE)
WALKER_ID_REGEX = compile(r"^w:([^:]*):([a-f\d]{24})$", IGNORECASE)
TA = TypeVar("TA", bound="type[Architype]")


@dataclass(eq=False)
class Anchor(_Anchor):
    """Object Anchor."""

    id: ObjectId = field(default_factory=ObjectId)  # type: ignore[assignment]
    root: Optional[ObjectId] = None  # type: ignore[assignment]
    access: Permission[ObjectId] = field(default_factory=Permission[ObjectId])  # type: ignore[assignment]
    architype: Optional[Architype] = None
    connected: bool = False

    # checker if needs to update on db
    changes: dict[str, dict[str, Any]] = field(default_factory=dict)

    class Collection(BaseCollection["Anchor"]):
        """Anchor collection interface."""

        __collection__: Optional[str] = "generic"
        __default_indexes__: list[dict] = [
            {"keys": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
        ]

    @staticmethod
    def ref(ref_id: str) -> Optional[Anchor]:
        """Return ObjectAnchor instance if ."""
        if matched := GENERIC_ID_REGEX.search(ref_id):
            cls: type = Anchor
            match AnchorType(matched.group(1)):
                case AnchorType.node:
                    cls = NodeAnchor
                case AnchorType.edge:
                    cls = EdgeAnchor
                case AnchorType.walker:
                    cls = WalkerAnchor
                case _:
                    pass
            return cls(name=matched.group(2), id=ObjectId(matched.group(3)))
        return None

    @property
    def _set(self) -> dict:
        if "$set" not in self.changes:
            self.changes["$set"] = {}
        return self.changes["$set"]

    @property
    def _add_to_set(self) -> dict:
        if "$addToSet" not in self.changes:
            self.changes["$addToSet"] = {}

        return self.changes["$addToSet"]

    @property
    def _pull(self) -> None:
        if "$pull" not in self.changes:
            self.changes["$pull"] = {}

        return self.changes["$pull"]

    def _add_to_set(self, field: str, ref_id: str, remove: bool = False) -> None:

        if field not in (add_to_set := self.changes["$addToSet"]):
            add_to_set[field] = {"$each": set()}

        ops: set = add_to_set[field]["$each"]

        if remove:
            if obj in ops:
                ops.remove(obj)
        else:
            ops.add(obj)
            self._pull(field, obj, True)

    def _pull(
        self, field: str, obj: Union["DocAnchor[DA]", ObjectId], remove: bool = False
    ) -> None:
        if "$pull" not in self.changes:
            self.changes["$pull"] = {}

        if field not in (pull := self.changes["$pull"]):
            pull[field] = {"$in": set()}

        ops: set = pull[field]["$in"]

        if remove:
            if obj in ops:
                ops.remove(obj)
        else:
            ops.add(obj)
            self._add_to_set(field, obj, True)

    def connect_edge(self, doc_anc: "DocAnchor[DA]", rollback: bool = False) -> None:
        """Push update that there's newly added edge."""
        if not rollback:
            self._add_to_set("edge", doc_anc)
        else:
            self._pull("edge", doc_anc, True)

    def disconnect_edge(self, doc_anc: "DocAnchor[DA]") -> None:
        """Push update that there's edge that has been removed."""
        self._pull("edge", doc_anc)

    def allow_node(self, node_id: ObjectId, write: bool = False) -> None:
        """Allow target node to access current Architype."""
        w = 1 if write else 0
        if node_id not in (nodes := self.access.nodes[w]):
            nodes.add(node_id)
            self._add_to_set(f"access.nodes.{w}", node_id)

    def disallow_node(self, node_id: ObjectId) -> None:
        """Remove target node access from current Architype."""
        for w in range(0, 2):
            if node_id in (nodes := self.access.nodes[w]):
                nodes.remove(node_id)
                self._pull(f"access.nodes.{w}", node_id)

    def allow_root(self, root_id: ObjectId, write: bool = False) -> None:
        """Allow all access from target root graph to current Architype."""
        w = 1 if write else 0
        if root_id not in (roots := self.access.roots[w]):
            roots.add(root_id)
            self._add_to_set(f"access.roots.{w}", root_id)

    def disallow_root(self, root_id: ObjectId) -> None:
        """Disallow all access from target root graph to current Architype."""
        for w in range(0, 2):
            if root_id in (roots := self.access.roots[w]):
                roots.remove(root_id)
                self._pull(f"access.roots.{w}", root_id)

    def unrestrict(self, write: bool = False) -> None:
        """Allow everyone to access current Architype."""
        w = 2 if write else 1
        if w > self.access.all:
            self.access.all = w
            self._set.update({"access.all": w})

    def restrict(self) -> None:
        """Disallow others to access current Architype."""
        if self.access.all:
            self.access.all = 0
            self._set.update({"access.all": 0})

    async def sync(self, node: Optional["NodeAnchor"] = None) -> Optional[Architype]:  # type: ignore[override]
        """Retrieve the Architype from db and return."""
        if architype := self.architype:
            if (node or self).has_read_access(self):
                return architype
            return None

        from .context import JaseciContext

        jsrc = JaseciContext.get().datasource
        anchor = await jsrc.find_one(self.type, self.id)

        if anchor and (node or self).has_read_access(anchor):
            self.__dict__.update(anchor.__dict__)

        return self.architype

    def allocate(self) -> None:
        """Allocate hashes and memory."""
        from .context import JaseciContext

        jctx = JaseciContext.get()
        self.root = jctx.root.id
        jctx.datasource.set(self)

    async def save(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:  # type: ignore[override]
        """Save Anchor."""
        if self.architype:
            if session:
                if not self.connected:
                    self.connected = True
                    self.sync_hash()
                    await self.insert(session)
                elif self.current_access_level > 0 and self.hash != (
                    _hash := self.data_hash()
                ):
                    self.hash = _hash
                    await self.update(session)
            else:
                await self.save_with_session()

    async def save_with_session(self) -> None:
        """Upsert Architype with session."""
        async with await BaseCollection.get_session() as session:
            async with session.start_transaction():
                try:
                    await self.save(session)
                    await session.commit_transaction()
                except Exception:
                    await session.abort_transaction()
                    logger.exception("Error saving Anchor!")
                    raise

    async def insert(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:
        """Insert Anchor."""
        raise NotImplementedError("insert must be implemented in subclasses")

    async def update(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:
        """Update Anchor."""
        raise NotImplementedError("update must be implemented in subclasses")

    async def destroy(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:  # type: ignore[override]
        """Save Anchor."""
        raise NotImplementedError("destroy must be implemented in subclasses")

    def serialize(self) -> dict[str, object]:
        """Serialize Anchor."""
        return {
            "_id": self.id,
            "type": self.type.value,
            "name": self.name,
            "root": self.root,
            "access": self.access.serialize(),
            "architype": (
                asdict(self.architype)
                if is_dataclass(self.architype) and not isinstance(self.architype, type)
                else {}
            ),
        }


@dataclass(eq=False)
class NodeAnchor(Anchor):
    """Node Anchor."""

    type: AnchorType = AnchorType.node
    architype: Optional[NodeArchitype] = None
    edges: list[EdgeAnchor] = field(default_factory=list)

    edges_hashes: dict[str, int] = field(default_factory=dict)
    architype_hashes: dict[str, int] = field(default_factory=dict)

    class Collection(BaseCollection["NodeAnchor"]):
        """NodeAnchor collection interface."""

        __collection__: Optional[str] = "node"
        __default_indexes__: list[dict] = [
            {"keys": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
        ]

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "NodeAnchor":
            """Parse document to NodeAnchor."""
            doc = cast(dict, doc)
            architype = doc.pop("architype")
            anchor = NodeAnchor(
                edges=[e for edge in doc.pop("edges") if (e := EdgeAnchor.ref(edge))],
                access=Permission.deserialize(doc.pop("access")),
                **doc,
            )
            anchor.architype = NodeArchitype.get(doc.get("name") or "Root")(
                __jac__=anchor, **architype
            )
            anchor.sync_hash()
            return anchor

    @classmethod
    def ref(cls, ref_id: str) -> Optional[NodeAnchor]:
        """Return NodeAnchor instance if existing."""
        if match := NODE_ID_REGEX.search(ref_id):
            return cls(
                name=match.group(1),
                id=ObjectId(match.group(2)),
            )
        return None

    async def insert(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:
        """Insert Anchor."""
        for edge in self.edges:
            await edge.save(session)

        await self.Collection.insert_one(self.serialize(), session)

    async def update(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:
        """Insert Anchor."""
        for edge in self.edges:
            await edge.save(session)

        await self.Collection.insert_one(self.serialize(), session)

    async def destroy(self) -> None:  # type: ignore[override]
        """Delete Anchor."""
        if self.architype and self.current_access_level > 1:
            from .context import JaseciContext

            jsrc = JaseciContext.get().datasource
            for edge in self.edges:
                await edge.destroy()

            jsrc.remove(self)

    async def sync(  # type: ignore[override]
        self, node: Optional["NodeAnchor"] = None
    ) -> Optional[NodeArchitype]:
        """Retrieve the Architype from db and return."""
        return cast(Optional[NodeArchitype], await super().sync(node))

    def connect_node(self, nd: NodeAnchor, edg: EdgeAnchor) -> None:
        """Connect a node with given edge."""
        edg.attach(self, nd)

    async def get_edges(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list[EdgeArchitype]], list[EdgeArchitype]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list[EdgeArchitype]:
        """Get edges connected to this node."""
        ret_edges: list[EdgeArchitype] = []
        for anchor in self.edges:
            if (
                (architype := await anchor.sync(self))
                and (source := anchor.source)
                and (target := anchor.target)
                and (not filter_func or filter_func([architype]))
            ):
                src_arch = await source.sync()
                trg_arch = await target.sync()

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

    async def edges_to_nodes(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list[EdgeArchitype]], list[EdgeArchitype]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list[NodeArchitype]:
        """Get set of nodes connected to this node."""
        ret_edges: list[NodeArchitype] = []
        for anchor in self.edges:
            if (
                (architype := await anchor.sync(self))
                and (source := anchor.source)
                and (target := anchor.target)
                and (not filter_func or filter_func([architype]))
            ):
                src_arch = await source.sync()
                trg_arch = await target.sync()

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

        collect_node_connections(self, visited_nodes, connections)  # type: ignore[arg-type]
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

    async def spawn_call(self, walk: WalkerAnchor) -> WalkerArchitype:
        """Invoke data spatial call."""
        return await walk.spawn_call(self)

    def serialize(self) -> dict[str, object]:
        """Serialize Node Anchor."""
        return {**super().serialize(), "edges": [edge.ref_id for edge in self.edges]}


@dataclass(eq=False)
class EdgeAnchor(Anchor):
    """Edge Anchor."""

    type: AnchorType = AnchorType.edge
    architype: Optional[EdgeArchitype] = None
    source: Optional[NodeAnchor] = None
    target: Optional[NodeAnchor] = None
    is_undirected: bool = False

    class Collection(BaseCollection["EdgeAnchor"]):
        """EdgeAnchor collection interface."""

        __collection__: Optional[str] = "edge"
        __default_indexes__: list[dict] = [
            {"keys": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
        ]

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "EdgeAnchor":
            """Parse document to EdgeAnchor."""
            doc = cast(dict, doc)
            architype = doc.pop("architype")
            anchor = EdgeAnchor(
                source=NodeAnchor.ref(doc.pop("source")),
                target=NodeAnchor.ref(doc.pop("target")),
                access=Permission.deserialize(doc.pop("access")),
                **doc,
            )
            anchor.architype = EdgeArchitype.get(doc.get("name") or "GenericEdge")(
                __jac__=anchor, **architype
            )
            return anchor

    @classmethod
    def ref(cls, ref_id: str) -> Optional[EdgeAnchor]:
        """Return EdgeAnchor instance if existing."""
        if match := EDGE_ID_REGEX.search(ref_id):
            return cls(
                name=match.group(1),
                id=ObjectId(match.group(2)),
            )
        return None

    async def destroy(self) -> None:  # type: ignore[override]
        """Delete Anchor."""
        if self.architype and self.current_access_level == 1:
            from .context import JaseciContext

            jsrc = JaseciContext.get().datasource

            source = self.source
            target = self.target
            self.detach()

            if source:
                await source.save()
            if target:
                await target.save()

            jsrc.remove(self)

    async def sync(  # type: ignore[override]
        self, node: Optional["NodeAnchor"] = None
    ) -> Optional[EdgeArchitype]:
        """Retrieve the Architype from db and return."""
        return cast(Optional[EdgeArchitype], await super().sync(node))

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

    async def spawn_call(self, walk: WalkerAnchor) -> WalkerArchitype:
        """Invoke data spatial call."""
        if target := self.target:
            return await walk.spawn_call(target)
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
class WalkerAnchor(Anchor):
    """Walker Anchor."""

    type: AnchorType = AnchorType.walker
    architype: Optional[WalkerArchitype] = None
    path: list[Anchor] = field(default_factory=list)
    next: list[Anchor] = field(default_factory=list)
    returns: list[Any] = field(default_factory=list)
    ignores: list[Anchor] = field(default_factory=list)
    disengaged: bool = False
    persistent: bool = False  # Disabled initially but can be adjusted

    class Collection(BaseCollection["WalkerAnchor"]):
        """WalkerAnchor collection interface."""

        __collection__: Optional[str] = "walker"
        __default_indexes__: list[dict] = [
            {"keys": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
        ]

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "WalkerAnchor":
            """Parse document to WalkerAnchor."""
            doc = cast(dict, doc)
            architype = doc.pop("architype")
            anchor = WalkerAnchor(
                access=Permission.deserialize(doc.pop("access")),
                **doc,
            )
            anchor.architype = WalkerArchitype.get(doc.get("name") or "")(
                __jac__=anchor, **architype
            )
            anchor.sync_hash()
            return anchor

    @classmethod
    def ref(cls, ref_id: str) -> Optional[WalkerAnchor]:
        """Return EdgeAnchor instance if existing."""
        if ref_id and (match := WALKER_ID_REGEX.search(ref_id)):
            return cls(
                name=match.group(1),
                id=ObjectId(match.group(2)),
            )
        return None

    async def destroy(self) -> None:  # type: ignore[override]
        """Delete Anchor."""
        if self.architype and self.current_access_level > 1:
            from .context import JaseciContext

            JaseciContext.get().datasource.remove(self)

    async def sync(self, node: Optional["NodeAnchor"] = None) -> Optional[WalkerArchitype]:  # type: ignore[override]
        """Retrieve the Architype from db and return."""
        return cast(Optional[WalkerArchitype], await super().sync(node))

    async def visit_node(self, anchors: Iterable[NodeAnchor | EdgeAnchor]) -> bool:
        """Walker visits node."""
        before_len = len(self.next)
        for anchor in anchors:
            if anchor not in self.ignores:
                if isinstance(anchor, NodeAnchor):
                    self.next.append(anchor)
                elif isinstance(anchor, EdgeAnchor):
                    if await anchor.sync() and (target := anchor.target):
                        self.next.append(target)
                    else:
                        raise ValueError("Edge has no target.")
        return len(self.next) > before_len

    async def ignore_node(self, anchors: Iterable[NodeAnchor | EdgeAnchor]) -> bool:
        """Walker ignores node."""
        before_len = len(self.ignores)
        for anchor in anchors:
            if anchor not in self.ignores:
                if isinstance(anchor, NodeAnchor):
                    self.ignores.append(anchor)
                elif isinstance(anchor, EdgeAnchor):
                    if await anchor.sync() and (target := anchor.target):
                        self.ignores.append(target)
                    else:
                        raise ValueError("Edge has no target.")
        return len(self.ignores) > before_len

    def disengage_now(self) -> None:
        """Disengage walker from traversal."""
        self.disengaged = True

    async def await_if_coroutine(self, ret: Any) -> Any:  # noqa: ANN401
        """Await return if it's a coroutine."""
        if iscoroutine(ret):
            ret = await ret

        self.returns.append(ret)

        return ret

    async def spawn_call(self, nd: Anchor) -> WalkerArchitype:
        """Invoke data spatial call."""
        if walker := await self.sync():
            self.path = []
            self.next = [nd]
            self.returns = []
            while len(self.next):
                if node := await self.next.pop(0).sync():
                    for i in node._jac_entry_funcs_:
                        if not i.trigger or isinstance(walker, i.trigger):
                            if i.func:
                                await self.await_if_coroutine(i.func(node, walker))
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
                    for i in walker._jac_entry_funcs_:
                        if not i.trigger or isinstance(node, i.trigger):
                            if i.func:
                                await self.await_if_coroutine(i.func(walker, node))
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
                    for i in walker._jac_exit_funcs_:
                        if not i.trigger or isinstance(node, i.trigger):
                            if i.func:
                                await self.await_if_coroutine(i.func(walker, node))
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
                    for i in node._jac_exit_funcs_:
                        if not i.trigger or isinstance(walker, i.trigger):
                            if i.func:
                                await self.await_if_coroutine(i.func(node, walker))
                            else:
                                raise ValueError(f"No function {i.name} to call.")
                        if self.disengaged:
                            return walker
            self.ignores = []
            return walker
        raise Exception(f"Invalid Reference {self.ref_id}")


class Architype(_Architype):
    """Architype Protocol."""

    __jac__: Anchor

    def __init__(self, __jac__: Optional[Anchor] = None) -> None:
        """Create default architype."""
        self.__jac__ = __jac__ or Anchor(architype=self)
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
