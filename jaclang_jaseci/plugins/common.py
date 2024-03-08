"""Common Classes for FastAPI Graph Integration."""

from contextvars import ContextVar
from copy import copy, deepcopy
from dataclasses import asdict, dataclass, field
from enum import Enum
from re import IGNORECASE, compile
from typing import Any, Callable, Mapping, Optional, Type, Union, cast

from bson import ObjectId

from fastapi import Request

from jaclang.core.construct import (
    EdgeAnchor as _EdgeAnchor,
    EdgeArchitype as _EdgeArchitype,
    EdgeDir,
    NodeAnchor as _NodeAnchor,
    NodeArchitype as _NodeArchitype,
    Root as _Root,
    root as base_root,
)

from motor.motor_asyncio import AsyncIOMotorClientSession

from orjson import dumps

from pymongo import ASCENDING, DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne

from ..collections import BaseCollection
from ..utils import logger


TARGET_NODE_REGEX = compile(r"^(n|e):([^:]*):([a-f\d]{24})$", IGNORECASE)
JCONTEXT: ContextVar = ContextVar("JCONTEXT")


class JType(Enum):
    """Enum For Graph Types."""

    node = "n"
    edge = "e"


class ArchCollection(BaseCollection):
    """Default Collection for Architypes."""

    @classmethod
    def build_node(
        cls, doc_anc: "DocAnchor", doc: Mapping[str, Any]
    ) -> "NodeArchitype":
        """Translate EdgeArchitypes edges into DocAnchor edges."""
        arch: NodeArchitype = doc_anc.build(**(doc.get("context") or {}))
        arch._jac_.edges = [DocAnchor.ref(edge) for edge in (doc.get("edge") or [])]

        return arch

    @classmethod
    def build_edge(
        cls, doc_anc: "DocAnchor", doc: Mapping[str, Any]
    ) -> "EdgeArchitype":
        """Build EdgeArchitypes from document."""
        arch: EdgeArchitype = doc_anc.build(**(doc.get("context") or {}))
        if src := doc.get("source"):
            arch._jac_.source = DocAnchor.ref(src)

        if tgt := doc.get("target"):
            arch._jac_.target = DocAnchor.ref(tgt)

        arch._jac_.is_undirected = bool(doc.get("is_undirected"))

        return arch


@dataclass
class DocAccess:
    """DocAnchor for Access Handler."""

    all: bool = False
    nodes: set[ObjectId] = field(default_factory=set)
    roots: set[ObjectId] = field(default_factory=set)

    def json(self) -> dict:
        """Return in dictionary type."""
        return {"all": self.all, "nodes": list(self.nodes), "roots": list(self.roots)}


@dataclass
class DocAnchor:
    """DocAnchor for Mongodb Referencing."""

    type: JType
    name: str = ""
    id: ObjectId = field(default_factory=ObjectId)
    root: Optional[ObjectId] = None
    access: DocAccess = field(default_factory=DocAccess)
    connected: bool = False
    arch: Optional[Union["NodeArchitype", "EdgeArchitype"]] = None
    changes: dict[str, dict[str, Any]] = field(default_factory=dict)
    hashes: dict[str, int] = field(default_factory=dict)
    rollback_changes: dict[str, dict[str, Any]] = field(default_factory=dict)
    rollback_hashes: dict[str, int] = field(default_factory=dict)

    @property
    def ref_id(self) -> str:
        """Return id in reference type."""
        return f"{self.type.value}:{self.name}:{self.id}"

    @property
    def _set(self) -> dict:
        if "$set" not in self.changes:
            self.changes["$set"] = {}
        return self.changes["$set"]

    def _add_to_set(
        self, field: str, obj: Union["DocAnchor", ObjectId], remove: bool = False
    ) -> None:
        if "$addToSet" not in self.changes:
            self.changes["$addToSet"] = {}

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
        self, field: str, obj: Union["DocAnchor", ObjectId], remove: bool = False
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

    def connect_edge(self, doc_anc: "DocAnchor", rollback: bool = False) -> None:
        """Push update that there's newly added edge."""
        if not rollback:
            self._add_to_set("edge", doc_anc)
        else:
            self._pull("edge", doc_anc, True)

    def disconnect_edge(self, doc_anc: "DocAnchor") -> None:
        """Push update that there's edge that has been removed."""
        self._pull("edge", doc_anc)

    def allow_node(self, node_id: ObjectId) -> None:
        """Allow target node to access current Architype."""
        if node_id not in self.access.nodes:
            self.access.nodes.add(node_id)
            self._add_to_set("access.nodes", node_id)

    def disallow_node(self, node_id: ObjectId) -> None:
        """Remove target node access from current Architype."""
        if node_id in self.access.nodes:
            self.access.nodes.remove(node_id)
            self._pull("access.nodes", node_id)

    def allow_root(self, root_id: ObjectId) -> None:
        """Allow all access from target root graph to current Architype."""
        if root_id not in self.access.roots:
            self.access.roots.add(root_id)
            self._add_to_set("access.roots", root_id)

    def disallow_root(self, root_id: ObjectId) -> None:
        """Disallow all access from target root graph to current Architype."""
        if root_id in self.access.roots:
            self.access.roots.remove(root_id)
            self._pull("access.roots", root_id)

    def unrestrict(self) -> None:
        """Allow everyone to access current Architype."""
        if not self.access.all:
            self.access.all = True
            self._set.update({"access.all": True})

    def restrict(self) -> None:
        """Disallow others to access current Architype."""
        if self.access.all:
            self.access.all = False
            self._set.update({"access.all": False})

    def class_ref(self) -> Type:
        """Return generated class equivalent for DocAnchor."""
        return JCLASS[self.type.value].get(self.name, DocArchitype)

    def pull_changes(self) -> dict:
        """Return changes and clear current reference."""
        self.rollback_changes = deepcopy(self.changes)
        self.rollback_hashes = copy(self.hashes)

        changes = self.changes
        _set = changes.pop("$set", {})
        self.changes = {}  # renew reference

        if self.arch:
            for key, val in asdict(self.arch).items():
                if (h := hash(dumps(val))) != self.hashes.get(key):
                    self.hashes[key] = h
                    _set[f"context.{key}"] = val

        if _set:
            changes["$set"] = _set

        return changes

    def rollback(self) -> None:
        """Rollback hashes so set update still available."""
        self.hashes = self.rollback_hashes
        self.changes = self.rollback_changes

    def build(
        self, **kwargs: Any  # noqa: ANN401
    ) -> Union["NodeArchitype", "EdgeArchitype"]:
        """Return generated class instance equivalent for DocAnchor."""
        arch = self.arch = self.class_ref()(**kwargs)

        if isinstance(arch, DocArchitype):
            arch._jac_doc_ = self

        return arch

    def json(self) -> dict:
        """Return in dictionary type."""
        return {
            "_id": self.id,
            "name": self.name,
            "root": self.root,
            "access": self.access.json(),
        }

    @classmethod
    def ref(cls, ref_id: str) -> Optional["DocAnchor"]:
        """Return DocAnchor instance if ."""
        if ref_id and (match := TARGET_NODE_REGEX.search(ref_id)):
            return cls(
                type=JType(match.group(1)),
                name=match.group(2),
                id=ObjectId(match.group(3)),
            )
        return None

    async def connect(self) -> Optional[Union["NodeArchitype", "EdgeArchitype"]]:
        """Retrieve the Architype from db and return."""
        data = None
        jctx: JacContext = JCONTEXT.get()

        if obj := jctx.get(self.id):
            data = self.arch = obj
            return data

        cls = self.class_ref()
        if (
            cls
            and (data := await cls.Collection.find_by_id(self.id))
            and isinstance(data, DocArchitype)
        ):
            self.arch = data
            jctx.set(data._jac_doc_.id, data)
        return data

    def __eq__(self, other: object) -> bool:
        """Override equal implementation."""
        if isinstance(other, DocAnchor):
            return (
                self.type == other.type
                and self.name == other.name
                and self.id == other.id
            )
        elif isinstance(other, DocArchitype):
            return self == other._jac_doc_

        return False

    def __hash__(self) -> int:
        """Override hash implementation."""
        return hash(self.ref_id)

    def __deepcopy__(self, memo: dict) -> "DocAnchor":
        """Override deepcopy implementation."""
        memo[id(self)] = self
        return self


class DocArchitype:
    """DocAnchor Class Handler."""

    _jac_type_: JType

    @property
    def _jac_doc_(self) -> DocAnchor:
        if isinstance(jd := getattr(self, "__jac_doc__", None), DocAnchor):
            return jd

        jctx: JacContext = JCONTEXT.get()
        jd = self.__jac_doc__ = DocAnchor(
            type=self._jac_type_,
            name=self.__class__.__name__,
            root=jctx.get_root_id(),
        )
        if isinstance(self, (NodeArchitype, EdgeArchitype)):
            jd.arch = self

        return jd

    @_jac_doc_.setter
    def _jac_doc_(self, val: DocAnchor) -> None:
        self.__jac_doc__ = val

    def get_context(self) -> dict:
        """Retrieve context dictionary."""
        ctx = {}
        if isinstance(self, (NodeArchitype, EdgeArchitype)):
            ctx = asdict(self)

        return {"context": ctx}

    async def propagate_save(
        self, changes: dict, session: AsyncIOMotorClientSession
    ) -> list[InsertOne[Any] | DeleteMany | DeleteOne | UpdateMany | UpdateOne]:
        """Propagate saving."""
        _ops: list[tuple[dict[str, ObjectId], dict[str, Any]]] = []

        jd_id = self._jac_doc_.id

        for ops in [
            ("$pull", "$in"),
            ("$addToSet", "$each"),
        ]:
            _list = None
            target = changes.get(ops[0], {})
            for op in target.values():
                if _set := op[ops[1]]:
                    _list = op[ops[1]] = list(_set)
                    for idx, danch in enumerate(_list):
                        if not danch.connected:
                            await danch.arch.save(session)
                        _list[idx] = danch.ref_id
            if _list:
                _ops.append(({"_id": jd_id}, {ops[0]: target}))

        if _set := changes.get("$set"):
            if _ops:
                _ops[0][1].update({"$set": _set})
            else:
                _ops.append(({"_id": jd_id}, {"$set": _set}))

        return [UpdateOne(*_op) for _op in _ops]

    async def save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor:
        """Upsert Architype."""
        raise Exception("Save function not implemented yet!")

    async def save_with_session(self) -> None:
        """Upsert Architype with session."""
        async with await ArchCollection.get_session() as session:
            async with session.start_transaction():
                try:
                    await self.save(session)
                    await session.commit_transaction()
                except Exception:
                    await session.abort_transaction()
                    logger.exception("Error saving node!")
                    raise

    async def destroy(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy Architype."""
        raise Exception("Destroy function not implemented yet!")

    async def destroy_with_session(self) -> None:
        """Destroy Architype with session."""
        async with await ArchCollection.get_session() as session:
            async with session.start_transaction():
                try:
                    await self.destroy(session)
                    await session.commit_transaction()
                except Exception:
                    await session.abort_transaction()
                    logger.exception(f"Error destroying {self._jac_type_.name}!")
                    raise

    def __eq__(self, other: object) -> bool:
        """Override equal implementation."""
        if isinstance(other, DocArchitype):
            return self._jac_doc_ == other._jac_doc_
        elif isinstance(other, DocAnchor):
            return self._jac_doc_ == other

        return False

    def __hash__(self) -> int:
        """Override hash implementation."""
        return self._jac_doc_.__hash__()


class NodeArchitype(_NodeArchitype, DocArchitype):
    """Overriden NodeArchitype."""

    _jac_type_ = JType.node

    def __init__(self) -> None:
        """Create node architype."""
        self._jac_: NodeAnchor = NodeAnchor(obj=self)

    class Collection(ArchCollection):
        """Default NodeArchitype Collection."""

        __collection__ = "node"
        __indexes__ = [
            {"fields": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
        ]

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "NodeArchitype":
            """Return parsed NodeArchitype from document."""
            access = cast(dict, doc.get("access"))
            return cls.build_node(
                DocAnchor(
                    type=JType.node,
                    name=cast(str, doc.get("name")),
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess(
                        all=cast(bool, access.get("all")),
                        nodes=set(cast(list, access.get("nodes"))),
                        roots=set(cast(list, access.get("roots"))),
                    ),
                    connected=True,
                    hashes={
                        key: hash(dumps(val))
                        for key, val in doc.get("context", {}).items()
                    },
                ),
                doc,
            )

    def connect_edge(self, edge: "EdgeArchitype", rollback: bool = False) -> None:
        """Update DocAnchor that there's newly added edge."""
        if isinstance(jd := getattr(self, "__jac_doc__", None), DocAnchor):
            jd.connect_edge(edge._jac_doc_, rollback)

    def disconnect_edge(self, edge: "EdgeArchitype") -> None:
        """Update DocAnchor that there's edge that has been removed."""
        if isinstance(jd := getattr(self, "__jac_doc__", None), DocAnchor):
            jd.disconnect_edge(edge._jac_doc_)

    async def destroy_edges(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy all EdgeArchitypes."""
        edges = self._jac_.edges
        jctx: JacContext = JCONTEXT.get()
        await jctx.populate(edges)
        for edge in edges:
            if isinstance(edge, DocAnchor):
                edge = await edge.connect()
            await edge.destroy(session)

    async def destroy(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy NodeArchitype."""
        if session:
            if (jd := self._jac_doc_).connected:
                await self.destroy_edges(session)
                await self.Collection.delete_by_id(jd.id, session)
                jd.connected = False
            else:
                await self.destroy_edges(session)
        else:
            await self.destroy_with_session()

    async def save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor:
        """Upsert NodeArchitype."""
        if session:
            jd = self._jac_doc_
            if not jd.connected:
                try:
                    jd.connected = True
                    jd.changes = {}
                    edges = [
                        (await edge.save(session)).ref_id for edge in self._jac_.edges
                    ]
                    await self.Collection.insert_one(
                        {**jd.json(), "edge": edges, **self.get_context()},
                        session=session,
                    )
                except Exception:
                    jd.connected = False
                    raise
            elif changes := jd.pull_changes():
                try:
                    await self.Collection.bulk_write(
                        await self.propagate_save(changes, session),
                        session=session,
                    )
                except Exception:
                    jd.rollback()
                    raise
        else:
            await self.save_with_session()

        return self._jac_doc_


@dataclass(eq=False)
class NodeAnchor(_NodeAnchor):
    """Overridden NodeAnchor."""

    async def get_edges(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list["EdgeArchitype"]], list["EdgeArchitype"]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list["EdgeArchitype"]:
        """Get edges connected to this node."""
        jctx: JacContext = JCONTEXT.get()
        await jctx.populate(self.edges)

        ret_edges: list[EdgeArchitype] = []
        async for s, t, e in (
            (src, tgt, ed)
            async for ed in (
                await edge.connect() if isinstance(edge, DocAnchor) else edge
                for edge in self.edges
            )
            if (src := ed._jac_.source)
            and (tgt := ed._jac_.target)
            and (
                not filter_func or filter_func([ed])
            )  # to be update once filter func allow non list
        ):
            if (
                dir in [EdgeDir.OUT, EdgeDir.ANY]
                and self.obj == s
                and (not target_obj or t.__class__ in target_obj)
            ):
                ret_edges.append(e)
            if (
                dir in [EdgeDir.IN, EdgeDir.ANY]
                and self.obj == t
                and (not target_obj or s.__class__ in target_obj)
            ):
                ret_edges.append(e)
        return ret_edges

    async def edges_to_nodes(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list["EdgeArchitype"]], list["EdgeArchitype"]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list[NodeArchitype]:
        """Get set of nodes connected to this node."""
        jctx: JacContext = JCONTEXT.get()
        await jctx.populate(self.edges)

        ret_nodes: list[EdgeArchitype] = []

        async for s, t, e in (
            (src, tgt, ed)
            async for ed in (
                await edge.connect() if isinstance(edge, DocAnchor) else edge
                for edge in self.edges
            )
            if (src := ed._jac_.source)
            and (tgt := ed._jac_.target)
            and (
                not filter_func or filter_func([ed])
            )  # to be update once filter func allow non list
        ):
            if (
                dir in [EdgeDir.OUT, EdgeDir.ANY]
                and self.obj == s
                and (not target_obj or t.__class__ in target_obj)
            ):
                if isinstance(t, DocAnchor):
                    e._jac_.target = t = await t.connect()
                ret_nodes.append(t)
            if (
                dir in [EdgeDir.IN, EdgeDir.ANY]
                and self.obj == t
                and (not target_obj or s.__class__ in target_obj)
            ):
                if isinstance(s, DocAnchor):
                    e._jac_.source = s = await s.connect()
                ret_nodes.append(s)
        return ret_nodes


@dataclass(eq=False)
class Root(NodeArchitype, _Root):
    """Overridden Root."""

    def __init__(self) -> None:
        """Create Root."""
        self._jac_: NodeAnchor = NodeAnchor(obj=self)

    @classmethod
    async def register(
        cls, session: Optional[AsyncIOMotorClientSession] = None
    ) -> "DocAnchor":
        """Register Root."""
        root = cls()
        root_id = ObjectId()
        root._jac_doc_ = DocAnchor(
            type=JType.node,
            id=root_id,
            root=root_id,
            arch=root,
        )
        return await root.save(session)

    def get_context(self) -> dict:
        """Override context retrieval."""
        return {}

    class Collection(NodeArchitype.Collection):
        """Default Root Collection."""

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "Root":
            """Return parsed NodeArchitype from document."""
            access = cast(dict, doc.get("access"))
            return cls.build_node(
                DocAnchor(
                    type=JType.node,
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess(
                        all=cast(bool, access.get("all")),
                        nodes=set(cast(list, access.get("nodes"))),
                        roots=set(cast(list, access.get("roots"))),
                    ),
                    connected=True,
                    hashes={
                        key: hash(dumps(val))
                        for key, val in doc.get("context", {}).items()
                    },
                ),
                doc,
            )


class EdgeArchitype(_EdgeArchitype, DocArchitype):
    """Overriden EdgeArchitype."""

    _jac_type_ = JType.edge

    def __init__(self) -> None:
        """Create EdgeArchitype."""
        self._jac_: EdgeAnchor = EdgeAnchor(obj=self)

    class Collection(ArchCollection):
        """Default EdgeArchitype Collection."""

        __collection__ = "edge"
        __indexes__ = [
            {"fields": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
        ]

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "EdgeArchitype":
            """Return parsed EdgeArchitype from document."""
            access = cast(dict, doc.get("access"))
            return cls.build_edge(
                DocAnchor(
                    type=JType.edge,
                    name=cast(str, doc.get("name")),
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess(
                        all=cast(bool, access.get("all")),
                        nodes=set(cast(list, access.get("nodes"))),
                        roots=set(cast(list, access.get("roots"))),
                    ),
                    connected=True,
                    hashes={
                        key: hash(dumps(val))
                        for key, val in doc.get("context", {}).items()
                    },
                ),
                doc,
            )

    async def destroy(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy EdgeArchitype."""
        if session:
            ea = self._jac_
            if (jd := self._jac_doc_).connected:
                try:
                    await ea.detach()
                    await ea.source.save(session)  # type: ignore[union-attr]
                    await ea.target.save(session)  # type: ignore[union-attr]
                    await self.Collection.delete_by_id(jd.id, session)
                    jd.connected = False
                except Exception:
                    await ea.reattach()
                    raise
            else:
                await ea.detach()
        else:
            await self.destroy_with_session()

    async def save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor:
        """Upsert EdgeArchitype."""
        if session:
            jd = self._jac_doc_
            if not jd.connected:
                try:
                    jd.connected = True
                    jd.changes = {}
                    await self.Collection.insert_one(
                        {
                            **jd.json(),
                            "source": (await self._jac_.source.save(session)).ref_id,  # type: ignore[union-attr]
                            "target": (await self._jac_.target.save(session)).ref_id,  # type: ignore[union-attr]
                            "is_undirected": self._jac_.is_undirected,
                            **self.get_context(),
                        },
                        session=session,
                    )
                except Exception:
                    jd.connected = False
                    raise
            elif changes := jd.pull_changes():
                try:
                    await self.Collection.update_by_id(
                        jd.id,
                        changes,
                        session=session,
                    )
                except Exception:
                    jd.rollback()
                    raise
        else:
            await self.save_with_session()

        return self._jac_doc_


@dataclass(eq=False)
class EdgeAnchor(_EdgeAnchor):
    """Overriden EdgeAnchor."""

    source: Optional[Union[NodeArchitype, DocAnchor]] = None
    target: Optional[Union[NodeArchitype, DocAnchor]] = None

    def attach(
        self, src: NodeArchitype, trg: NodeArchitype, is_undirected: bool = False
    ) -> "EdgeAnchor":
        """Attach edge to nodes."""
        self.source = src
        self.target = trg
        self.is_undirected = is_undirected
        src._jac_.edges.append(self.obj)
        src.connect_edge(self.obj)
        trg._jac_.edges.append(self.obj)
        trg.connect_edge(self.obj)
        return self

    async def reattach(self) -> None:
        """Reattach edge from nodes."""
        if isinstance(src := self.source, DocAnchor):
            src = self.source = await src.connect()

        if src:
            src._jac_.edges.append(self.obj)
            src.connect_edge(self.obj, True)

        if isinstance(tgt := self.target, DocAnchor):
            tgt = self.target = await tgt.connect()

        if tgt:
            tgt._jac_.edges.append(self.obj)
            tgt.connect_edge(self.obj, True)

    async def detach(self) -> None:
        """Detach edge from nodes."""
        if isinstance(src := self.source, DocAnchor):
            src = self.source = await src.connect()

        if src:
            src._jac_.edges.remove(self.obj)
            src.disconnect_edge(self.obj)

        if isinstance(tgt := self.target, DocAnchor):
            tgt = self.target = await tgt.connect()

        if tgt:
            tgt._jac_.edges.remove(self.obj)
            tgt.disconnect_edge(self.obj)


@dataclass(eq=False)
class GenericEdge(EdgeArchitype):
    """GenericEdge Replacement."""

    def __init__(self) -> None:
        """Create Generic Edge."""
        self._jac_: EdgeAnchor = EdgeAnchor(obj=self)

    def get_context(self) -> dict:
        """Override context retrieval."""
        return {}

    class Collection(EdgeArchitype.Collection):
        """Default GenericEdge Collection."""

        __collection__ = "edge"

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "EdgeArchitype":
            """Return parsed EdgeArchitype from document."""
            access = cast(dict, doc.get("access"))
            return cls.build_edge(
                DocAnchor(
                    type=JType.edge,
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess(
                        all=cast(bool, access.get("all")),
                        nodes=set(cast(list, access.get("nodes"))),
                        roots=set(cast(list, access.get("roots"))),
                    ),
                    connected=True,
                    hashes={
                        key: hash(dumps(val))
                        for key, val in doc.get("context", {}).items()
                    },
                ),
                doc,
            )


class JacContext:
    """Jac Lang Context Handler."""

    def __init__(self, request: Request, entry: Optional[str] = None) -> None:
        """Create JacContext."""
        self.__mem__: dict[ObjectId, Union[NodeArchitype, EdgeArchitype]] = {}
        self.request = request
        self.user = getattr(request, "auth_user", None)
        self.root: Root = getattr(request, "auth_root", base_root)
        self.reports: list[Any] = []
        self.entry = entry

    def get_root_id(self) -> Optional[ObjectId]:
        """Retrieve Root Doc Id."""
        if self.root is base_root:
            return None
        return self.root._jac_doc_.id

    async def get_entry(self) -> NodeArchitype:
        """Retrieve Node Entry Point."""
        if isinstance(self.entry, str):
            if self.entry and (match := TARGET_NODE_REGEX.search(self.entry)):
                entry = await cast(
                    Union[NodeArchitype, EdgeArchitype],
                    JCLASS[match.group(1)][match.group(2)],
                ).Collection.find_by_id(ObjectId(match.group(3)))
                if isinstance(entry, NodeArchitype):
                    self.entry = entry
                else:
                    self.entry = self.root
            else:
                self.entry = self.root
        elif self.entry is None:
            self.entry = self.root

        return self.entry

    async def populate(self, danchors: list[Union[DocArchitype, DocAnchor]]) -> None:
        """Populate in-memory references."""
        queue: dict[type, dict[str, dict]] = {}
        for danchor in danchors:
            if isinstance(danchor, DocAnchor) and not self.has(danchor.id):
                cls = danchor.class_ref()
                if cls not in queue:
                    queue[cls] = {"_id": {"$in": []}}
                qin: list = queue[cls]["_id"]["$in"]
                qin.append(danchor.id)
        for cls, que in queue.items():
            async for arch in await cls.Collection.find(que):
                self.set(arch._jac_doc_.id, arch)

    def has(self, id: Union[ObjectId, str]) -> bool:
        """Check if Architype is existing in memory."""
        return ObjectId(id) in self.__mem__

    def get(
        self,
        id: Union[ObjectId, str],
        default: Optional[Union["NodeArchitype", "EdgeArchitype"]] = None,
    ) -> Optional[Union["NodeArchitype", "EdgeArchitype"]]:
        """Retrieve Architype in memory."""
        return self.__mem__.get(ObjectId(id), default)

    def set(
        self, id: Union[ObjectId, str], obj: Union["NodeArchitype", "EdgeArchitype"]
    ) -> None:
        """Push Architype in memory via ID."""
        self.__mem__[ObjectId(id)] = obj

    def remove(
        self, id: Union[ObjectId, str]
    ) -> Optional[Union["NodeArchitype", "EdgeArchitype"]]:
        """Pull Architype in memory via ID."""
        return self.__mem__.pop(ObjectId(id), None)

    def report(self, obj: Any) -> None:  # noqa: ANN401
        """Append report."""
        self.reports.append(obj)

    def response(self, returns: list[Any], status: int = 200) -> dict[str, Any]:
        """Return serialized version of reports."""
        resp = {"status": status, "returns": returns}

        if self.reports:
            for key, val in enumerate(self.reports):
                if isinstance(val, DocArchitype) and (
                    ret_jd := getattr(val, "_jac_doc_", None)
                ):
                    self.reports[key] = {"id": ret_jd.ref_id, **val.get_context()}
                else:
                    self.clean_response(key, val, self.reports)
            resp["reports"] = self.reports

        return resp

    def clean_response(
        self, key: Union[str, int], val: Any, obj: Union[list, dict]  # noqa: ANN401
    ) -> None:
        """Cleanup and override current object."""
        if isinstance(val, list):
            for idx, lval in enumerate(val):
                self.clean_response(idx, lval, val)
        elif isinstance(val, dict):
            for key, dval in val.items():
                self.clean_response(key, dval, val)
        elif isinstance(val, DocArchitype):
            cast(dict, obj)[key] = {"id": val._jac_doc_.ref_id, **val.get_context()}


Root.__name__ = ""
GenericEdge.__name__ = ""

JCLASS: dict[str, dict[str, type]] = {"n": {"": Root}, "e": {"": GenericEdge}}
