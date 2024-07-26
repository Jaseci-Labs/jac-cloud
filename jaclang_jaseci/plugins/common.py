"""Common Classes for FastAPI Graph Integration."""

from asyncio import get_event_loop
from contextvars import ContextVar
from copy import copy, deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass
from enum import Enum
from os import getenv
from re import IGNORECASE, compile
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Generic,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from bson import ObjectId

from fastapi import Request
from fastapi.exceptions import HTTPException

from jaclang.core.construct import (
    EdgeAnchor as _EdgeAnchor,
    EdgeArchitype as _EdgeArchitype,
    EdgeDir,
    NodeAnchor as _NodeAnchor,
    NodeArchitype as _NodeArchitype,
    Root as _Root,
)

from motor.motor_asyncio import AsyncIOMotorClientSession

from orjson import dumps

from pymongo import ASCENDING, DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure

from ..collections import BaseCollection
from ..utils import logger


SHOW_ENDPOINT_RETURNS = getenv("SHOW_ENDPOINT_RETURNS", False)
SESSION_MAX_COMMIT_RETRY = int(getenv("SESSION_MAX_COMMIT_RETRY") or "1")
TARGET_NODE_REGEX = compile(r"^(n|e):([^:]*):([a-f\d]{24})$", IGNORECASE)
JCONTEXT: ContextVar = ContextVar("JCONTEXT")
T = TypeVar("T")
DA = TypeVar("DA", bound="DocArchitype")


async def commit_session_with_retry(session: AsyncIOMotorClientSession) -> None:
    """Commit session with retry."""
    retry = 0
    max_retry = SESSION_MAX_COMMIT_RETRY
    while retry <= max_retry:
        try:
            await session.commit_transaction()
            break
        except (ConnectionFailure, OperationFailure) as ex:
            if ex.has_error_label("UnknownTransactionCommitResult"):
                retry += 1
                logger.error(
                    "Error commiting session! " f"Retrying [{retry}/{max_retry}] ..."
                )
                continue
            logger.error(f"Error commiting session after max retry [{max_retry}] !")
            raise
        except Exception:
            await session.abort_transaction()
            logger.error("Error commiting session!")
            raise


class JType(Enum):
    """Enum For Graph Types."""

    node = "n"
    edge = "e"


class ArchCollection(BaseCollection[T]):
    """Default Collection for Architypes."""

    __default_indexes__: list[dict] = [
        {"keys": [("_id", ASCENDING), ("name", ASCENDING), ("root", ASCENDING)]}
    ]

    @classmethod
    def build_node(
        cls, doc_anc: "DocAnchor[NodeArchitype]", doc: Mapping[str, Any]
    ) -> "NodeArchitype":
        """Build NodeArchitypes from document."""
        arch: NodeArchitype = doc_anc.build(**(doc.get("context") or {}))
        arch._jac_.edges = [
            ed
            for edge in (doc.get("edge") or [])
            if (ed := DocAnchor[EdgeArchitype].ref(edge))
        ]

        return arch

    @classmethod
    def build_edge(
        cls, doc_anc: "DocAnchor[EdgeArchitype]", doc: Mapping[str, Any]
    ) -> "EdgeArchitype":
        """Build EdgeArchitypes from document."""
        arch: EdgeArchitype = doc_anc.build(**(doc.get("context") or {}))
        if src := doc.get("source"):
            arch._jac_.source = DocAnchor[NodeArchitype].ref(src)

        if tgt := doc.get("target"):
            arch._jac_.target = DocAnchor[NodeArchitype].ref(tgt)

        arch._jac_.is_undirected = bool(doc.get("is_undirected"))

        return arch


@dataclass
class DocAccess:
    """DocAnchor for Access Handler."""

    # 0 == not publicly accessible
    # 1 == publicly accessible without right access
    # 2 == publicly accessible with right access
    all: int = 0

    # tuple(
    #   [] == list of node ObjectId that has read access
    #   [] == list of node ObjectId that has right access
    # )
    nodes: tuple[set[ObjectId], set[ObjectId]] = field(
        default_factory=lambda: (set(), set())
    )

    # tuple(
    #   [] == list of user (via root_id) that has read access
    #   [] == list of user (via root_id) that has right access
    # )
    roots: tuple[set[ObjectId], set[ObjectId]] = field(
        default_factory=lambda: (set(), set())
    )

    @staticmethod
    def from_json(access: dict[str, Any]) -> "DocAccess":
        """Convert dict to DocAccess."""
        all = cast(int, access.get("all"))
        nodes = cast(list[list[ObjectId]], access.get("nodes") or [[], []])
        roots = cast(list[list[ObjectId]], access.get("roots") or [[], []])

        return DocAccess(
            all,
            (set(nodes[0]), set(nodes[1])),
            (set(roots[0]), set(roots[1])),
        )

    def json(self) -> dict:
        """Return in dictionary type."""
        return {
            "all": self.all,
            "nodes": [list(self.nodes[0]), list(self.nodes[1])],
            "roots": [list(self.roots[0]), list(self.roots[1])],
        }


@dataclass
class DocAnchor(Generic[DA]):
    """DocAnchor for Mongodb Referencing."""

    type: JType
    name: str = ""
    id: ObjectId = field(default_factory=ObjectId)
    root: Optional[ObjectId] = None
    access: DocAccess = field(default_factory=DocAccess)
    connected: bool = False
    arch: Optional["DocArchitype[DA]"] = None

    # checker if needs to update on db
    changes: dict[str, dict[str, Any]] = field(default_factory=dict)
    # context checker if update happens for each field
    hashes: dict[str, int] = field(default_factory=dict)
    # rollback holder if something happens on updating
    rollback_changes: dict[str, dict[str, Any]] = field(default_factory=dict)
    # rollback holder if something happens on updating
    rollback_hashes: dict[str, int] = field(default_factory=dict)

    # 0 == don't have access
    # 1 == with read access
    # 2 == with write access
    current_access_level: Optional[int] = None

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
        self, field: str, obj: Union["DocAnchor[DA]", ObjectId], remove: bool = False
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

    def class_ref(self) -> Type[DA]:
        """Return generated class equivalent for DocAnchor."""
        if self.type is JType.node:
            return JCLASS[self.type.value].get(self.name, NodeArchitype)
        return JCLASS[self.type.value].get(self.name, EdgeArchitype)

    def pull_changes(self) -> dict:
        """Return changes and clear current reference."""
        self.rollback_changes = deepcopy(self.changes)
        self.rollback_hashes = copy(self.hashes)

        changes = self.changes
        _set = changes.pop("$set", {})
        self.changes = {}  # renew reference

        if is_dataclass(self.arch) and not isinstance(self.arch, type):
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

    def build(self, **kwargs: Any) -> DA:  # noqa: ANN401
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
    def ref(cls, ref_id: str) -> Optional["DocAnchor[DA]"]:
        """Return DocAnchor instance if ."""
        if ref_id and (match := TARGET_NODE_REGEX.search(ref_id)):
            return cls(
                type=JType(match.group(1)),
                name=match.group(2),
                id=ObjectId(match.group(3)),
            )
        return None

    def connect(self, node: Optional["NodeArchitype"] = None) -> Optional[DA]:
        """Sync Retrieve the Architype from db and return."""
        return get_event_loop().run_until_complete(self._connect(node))

    async def _connect(self, node: Optional["NodeArchitype"] = None) -> Optional[DA]:
        """Retrieve the Architype from db and return."""
        jctx: JacContext = JacContext.get_context()

        if obj := jctx.get(self.id):
            self.arch = obj
            return obj

        cls = self.class_ref()
        if (
            cls
            and (data := await cls.Collection.find_by_id(self.id))
            and isinstance(data, (NodeArchitype, EdgeArchitype))
            and (
                await jctx.root.is_allowed(data)
                or (node and await node.is_allowed(data))
            )
        ):
            self.arch = data
            jctx.set(data._jac_doc_.id, data)

        if isinstance(data, (NodeArchitype, EdgeArchitype)):
            return data
        return None

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

    def __deepcopy__(self, memo: dict) -> "DocAnchor[DA]":
        """Override deepcopy implementation."""
        memo[id(self)] = self
        return self


class DocArchitype(Generic[DA]):
    """DocAnchor Class Handler."""

    _jac_type_: JType

    class Collection(ArchCollection):
        """Default DocArchitype Collection."""

        pass

    @property
    def _jac_auto_save_(self) -> bool:
        return isinstance(jctx := JCONTEXT.get(None), JacContext) and jctx.save_on_exit

    @property
    def _jac_doc_(self) -> DocAnchor[DA]:
        if isinstance(jd := getattr(self, "__jac_doc__", None), DocAnchor):
            return jd

        jctx: JacContext = JacContext.get_context()
        jd = self.__jac_doc__ = DocAnchor[DA](
            type=self._jac_type_,
            name=self.__class__.__name__,
            root=jctx.get_root_id(),
        )
        if isinstance(self, DocArchitype):
            jd.arch = self

        return jd

    @_jac_doc_.setter
    def _jac_doc_(self, val: DocAnchor[DA]) -> None:
        self.__jac_doc__ = val

    def get_context(self) -> dict:
        """Retrieve context dictionary."""
        ctx = {}
        if is_dataclass(self) and not isinstance(self, type):
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
                        if isinstance(danch, DocAnchor):
                            if danch.arch:
                                await danch.arch._save(session)
                            _list[idx] = danch.ref_id
            if _list:
                _ops.append(({"_id": jd_id}, {ops[0]: target}))

        if _set := changes.get("$set"):
            if _ops:
                _ops[0][1].update({"$set": _set})
            else:
                _ops.append(({"_id": jd_id}, {"$set": _set}))

        return [UpdateOne(*_op) for _op in _ops]

    def connect(self: DA) -> "DA":
        """Sync Return self."""
        return get_event_loop().run_until_complete(self._connect())

    async def _connect(self: DA) -> "DA":
        """Return self."""
        return self

    def save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor[DA]:
        """Sync upsert Architype."""
        return get_event_loop().run_until_complete(self._save(session))

    async def _save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor[DA]:
        """Upsert Architype."""
        raise Exception("Save function not implemented yet!")

    async def save_with_session(self) -> None:
        """Upsert Architype with session."""
        async with await ArchCollection.get_session() as session:
            async with session.start_transaction():
                await self._save(session)
                await commit_session_with_retry(session)

    def destroy(self, session: Optional[AsyncIOMotorClientSession] = None) -> None:
        """Sync Destroy Architype."""
        get_event_loop().run_until_complete(self._destroy(session))

    async def _destroy(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy Architype."""
        raise Exception("Destroy function not implemented yet!")

    async def destroy_with_session(self) -> None:
        """Destroy Architype with session."""
        async with await ArchCollection.get_session() as session:
            async with session.start_transaction():
                await self._destroy(session)
                await commit_session_with_retry(session)

    async def is_allowed(
        self, to: "DocArchitype", jctx: Optional["JacContext"] = None
    ) -> bool:
        """Access validation."""
        if not jctx:
            jctx = JacContext.get_context()
        if (
            not jctx
            or not (from_jd := self._jac_doc_)
            or not (to_jd := to._jac_doc_)
            or not (from_root := from_jd.root)
            or not (to_root := to_jd.root)
        ):
            return False

        jroot = jctx.root

        if (
            (isinstance(self, Root) and from_jd.id == to_root)
            or from_root == to_root
            or jroot._jac_doc_.id == to_root
            or jroot == to
        ):
            to_jd.current_access_level = 1
            return True

        if (to_access := to_jd.access).all:
            to_jd.current_access_level = to_access.all - 1
            return True

        for i in range(1, -1, -1):
            if from_jd.id in to_access.nodes[i] or from_root in to_access.roots[i]:
                to_jd.current_access_level = i
                return True

        if isinstance(
            to_root_access := await jctx.check_root_access(to_root),
            DocAccess,
        ) and (cur_root_id := jctx.get_root_id()):
            for i in range(1, -1, -1):
                if cur_root_id in to_root_access.roots[i]:
                    to_jd.current_access_level = i
                    return True

        to_jd.current_access_level = None
        return False

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


class NodeArchitype(_NodeArchitype, DocArchitype["NodeArchitype"]):
    """Overriden NodeArchitype."""

    _jac_type_ = JType.node

    def __init__(self) -> None:
        """Create node architype."""
        self._jac_: NodeAnchor = NodeAnchor(obj=self)
        JacContext.try_set(self)

    class Collection(ArchCollection["NodeArchitype"]):
        """Default NodeArchitype Collection."""

        __collection__ = "node"

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "NodeArchitype":
            """Return parsed NodeArchitype from document."""
            return cls.build_node(
                DocAnchor[NodeArchitype](
                    type=JType.node,
                    name=cast(str, doc.get("name")),
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess.from_json(cast(dict, doc.get("access"))),
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
        self._jac_doc_.connect_edge(edge._jac_doc_, rollback)

    def disconnect_edge(self, edge: "EdgeArchitype") -> None:
        """Update DocAnchor that there's edge that has been removed."""
        self._jac_doc_.disconnect_edge(edge._jac_doc_)

    def allow_node(
        self, node: Union[DocAnchor, "NodeArchitype"], write: bool = False
    ) -> None:
        """Allow target node to access current Architype."""
        if isinstance(node, DocAnchor):
            self._jac_doc_.allow_node(node.id, write)
        elif isinstance(node, NodeArchitype):
            self._jac_doc_.allow_node(node._jac_doc_.id, write)

    def disallow_node(self, node: Union[DocAnchor, "NodeArchitype"]) -> None:
        """Remove target node access from current Architype."""
        if isinstance(node, DocAnchor):
            self._jac_doc_.disallow_node(node.id)
        elif isinstance(node, NodeArchitype):
            self._jac_doc_.disallow_node(node._jac_doc_.id)

    def allow_root(self, root: Union[DocAnchor, "Root"], write: bool = False) -> None:
        """Allow all access from target root graph to current Architype."""
        if isinstance(root, DocAnchor):
            self._jac_doc_.allow_root(root.id, write)
        elif isinstance(root, Root):
            self._jac_doc_.allow_root(root._jac_doc_.id, write)

    def disallow_root(self, root: Union[DocAnchor, "Root"]) -> None:
        """Disallow all access from target root graph to current Architype."""
        if isinstance(root, DocAnchor):
            self._jac_doc_.disallow_root(root.id)
        elif isinstance(root, Root):
            self._jac_doc_.disallow_root(root._jac_doc_.id)

    def unrestrict(self, write: bool = False) -> None:
        """Allow everyone to access current Architype."""
        self._jac_doc_.unrestrict(write)

    def restrict(self) -> None:
        """Disallow others to access current Architype."""
        self._jac_doc_.restrict()

    async def destroy_edges(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy all EdgeArchitypes."""
        edges = self._jac_.edges
        jctx: JacContext = JacContext.get_context()
        await jctx.populate_edges(edges)
        for edge in edges:
            if ed := await edge._connect():
                await ed._destroy(session)

    async def _destroy(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy NodeArchitype."""
        if self._jac_auto_save_:
            await self.destroy_edges()
            return JacContext.try_destroy(self)
        if session:
            if (jd := self._jac_doc_).connected and jd.current_access_level == 1:
                await self.destroy_edges(session)
                await self.Collection.delete_by_id(jd.id, session)
                jd.connected = False
            else:
                await self.destroy_edges(session)
        else:
            await self.destroy_with_session()

    async def _save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor["NodeArchitype"]:
        """Upsert NodeArchitype."""
        if not self._jac_auto_save_:
            if session:
                jd = self._jac_doc_
                if not jd.connected:
                    try:
                        jd.connected = True
                        jd.changes = {}
                        edges = [
                            (await ed._save(session)).ref_id
                            for edge in self._jac_.edges
                            if (ed := await edge._connect())
                        ]
                        await self.Collection.insert_one(
                            {**jd.json(), "edge": edges, **self.get_context()},
                            session=session,
                        )
                    except Exception:
                        jd.connected = False
                        raise
                elif jd.current_access_level == 1 and (changes := jd.pull_changes()):
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

    obj: NodeArchitype
    edges: list[Union["EdgeArchitype", DocAnchor["EdgeArchitype"]]] = field(
        default_factory=list
    )

    async def get_edges(
        self,
        dir: EdgeDir,
        filter_func: Optional[Callable[[list["EdgeArchitype"]], list["EdgeArchitype"]]],
        target_obj: Optional[list[NodeArchitype]],
    ) -> list["EdgeArchitype"]:
        """Get edges connected to this node."""
        jctx: JacContext = JacContext.get_context()
        await jctx.populate_edges(self.edges)

        ret_edges: list[EdgeArchitype] = []
        async for e, s, t in async_filter(self.edges):
            if not filter_func or filter_func([e]):
                if (
                    dir in [EdgeDir.OUT, EdgeDir.ANY]
                    and self.obj == s
                    and (not target_obj or t.__class__ in target_obj)
                    and await s.is_allowed(e, jctx)
                    and await s.is_allowed(t, jctx)
                ):
                    ret_edges.append(e)
                if (
                    dir in [EdgeDir.IN, EdgeDir.ANY]
                    and self.obj == t
                    and (not target_obj or s.__class__ in target_obj)
                    and await t.is_allowed(e, jctx)
                    and await t.is_allowed(s, jctx)
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
        jctx: JacContext = JacContext.get_context()
        await jctx.populate_edges(self.edges)

        ret_nodes: list[NodeArchitype] = []
        async for e, s, t in async_filter(self.edges):
            if not filter_func or filter_func([e]):
                if (
                    dir in [EdgeDir.OUT, EdgeDir.ANY]
                    and self.obj == s
                    and (not target_obj or t.__class__ in target_obj)
                    and await s.is_allowed(e, jctx)
                    and await s.is_allowed(t, jctx)
                ):
                    ret_nodes.append(t)
                if (
                    dir in [EdgeDir.IN, EdgeDir.ANY]
                    and self.obj == t
                    and (not target_obj or s.__class__ in target_obj)
                    and await t.is_allowed(e, jctx)
                    and await t.is_allowed(s, jctx)
                ):
                    ret_nodes.append(s)

        return ret_nodes


@dataclass(eq=False)
class Root(NodeArchitype, _Root):
    """Overridden Root."""

    def __init__(self) -> None:
        """Create Root."""
        self._jac_: NodeAnchor = NodeAnchor(obj=self)
        JacContext.try_set(self)

    @classmethod
    async def register(
        cls, session: Optional[AsyncIOMotorClientSession] = None
    ) -> "DocAnchor[Root]":
        """Register Root."""
        root = cls()
        root_id = ObjectId()
        root._jac_doc_ = DocAnchor[Root](
            type=JType.node,
            id=root_id,
            root=root_id,
            arch=root,
        )
        return await root._save(session)

    def get_context(self) -> dict:
        """Override context retrieval."""
        return {}

    class Collection(ArchCollection["Root"]):
        """Default Root Collection."""

        __collection__ = "node"

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "Root":
            """Return parsed NodeArchitype from document."""
            return cls.build_node(
                DocAnchor[Root](
                    type=JType.node,
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess.from_json(cast(dict, doc.get("access"))),
                    connected=True,
                    hashes={
                        key: hash(dumps(val))
                        for key, val in doc.get("context", {}).items()
                    },
                ),
                doc,
            )


class EdgeArchitype(_EdgeArchitype, DocArchitype["EdgeArchitype"]):
    """Overriden EdgeArchitype."""

    _jac_type_ = JType.edge

    def __init__(self) -> None:
        """Create EdgeArchitype."""
        self._jac_: EdgeAnchor = EdgeAnchor(obj=self)
        JacContext.try_set(self)

    class Collection(ArchCollection["EdgeArchitype"]):
        """Default EdgeArchitype Collection."""

        __collection__ = "edge"

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "EdgeArchitype":
            """Return parsed EdgeArchitype from document."""
            return cls.build_edge(
                DocAnchor[EdgeArchitype](
                    type=JType.edge,
                    name=cast(str, doc.get("name")),
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess.from_json(cast(dict, doc.get("access"))),
                    connected=True,
                    hashes={
                        key: hash(dumps(val))
                        for key, val in doc.get("context", {}).items()
                    },
                ),
                doc,
            )

    async def _destroy(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> None:
        """Destroy EdgeArchitype."""
        if self._jac_auto_save_:
            await self._jac_.detach()
            return JacContext.try_destroy(self)
        if session:
            ea = self._jac_
            if (jd := self._jac_doc_).connected and jd.current_access_level == 1:
                try:
                    await ea.detach()
                    await ea.source._save(session)  # type: ignore[union-attr]
                    await ea.target._save(session)  # type: ignore[union-attr]
                    await self.Collection.delete_by_id(jd.id, session)
                    jd.connected = False
                except Exception:
                    await ea.reattach()
                    raise
            else:
                await ea.detach()
        else:
            await self.destroy_with_session()

    async def _save(
        self, session: Optional[AsyncIOMotorClientSession] = None
    ) -> DocAnchor["EdgeArchitype"]:
        """Upsert EdgeArchitype."""
        if not self._jac_auto_save_:
            if session:
                jd = self._jac_doc_
                if not jd.connected:
                    try:
                        jd.connected = True
                        jd.changes = {}
                        await self.Collection.insert_one(
                            {
                                **jd.json(),
                                "source": (await self._jac_.source._save(session)).ref_id,  # type: ignore[union-attr]
                                "target": (await self._jac_.target._save(session)).ref_id,  # type: ignore[union-attr]
                                "is_undirected": self._jac_.is_undirected,
                                **self.get_context(),
                            },
                            session=session,
                        )
                    except Exception:
                        jd.connected = False
                        raise
                elif jd.current_access_level == 1 and (changes := jd.pull_changes()):
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

    def allow_root(self, root: Union[DocAnchor, "Root"], write: bool = False) -> None:
        """Allow all access from target root graph to current Architype."""
        if isinstance(root, DocAnchor):
            self._jac_doc_.allow_root(root.id, write)
        elif isinstance(root, Root):
            self._jac_doc_.allow_root(root._jac_doc_.id, write)

    def disallow_root(self, root: Union[DocAnchor, "Root"]) -> None:
        """Disallow all access from target root graph to current Architype."""
        if isinstance(root, DocAnchor):
            self._jac_doc_.disallow_root(root.id)
        elif isinstance(root, Root):
            self._jac_doc_.disallow_root(root._jac_doc_.id)

    def unrestrict(self, write: bool = False) -> None:
        """Allow everyone to access current Architype."""
        self._jac_doc_.unrestrict(write)

    def restrict(self) -> None:
        """Disallow others to access current Architype."""
        self._jac_doc_.restrict()


@dataclass(eq=False)
class EdgeAnchor(_EdgeAnchor):
    """Overriden EdgeAnchor."""

    obj: EdgeArchitype
    source: Optional[Union[NodeArchitype, DocAnchor[NodeArchitype]]] = None
    target: Optional[Union[NodeArchitype, DocAnchor[NodeArchitype]]] = None

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
            src = self.source = await src._connect()

        if src:
            src._jac_.edges.append(self.obj)
            src.connect_edge(self.obj, True)

        if isinstance(tgt := self.target, DocAnchor):
            tgt = self.target = await tgt._connect()

        if tgt:
            tgt._jac_.edges.append(self.obj)
            tgt.connect_edge(self.obj, True)

    async def detach(self) -> None:
        """Detach edge from nodes."""
        if isinstance(src := self.source, DocAnchor):
            src = self.source = await src._connect()

        if src and self.obj in (ses := src._jac_.edges):
            ses.remove(self.obj)
            src.disconnect_edge(self.obj)

        if isinstance(tgt := self.target, DocAnchor):
            tgt = self.target = await tgt._connect()

        if tgt and self.obj in (tes := tgt._jac_.edges):
            tes.remove(self.obj)
            tgt.disconnect_edge(self.obj)


@dataclass(eq=False)
class GenericEdge(EdgeArchitype):
    """GenericEdge Replacement."""

    def __init__(self) -> None:
        """Create Generic Edge."""
        self._jac_: EdgeAnchor = EdgeAnchor(obj=self)
        JacContext.try_set(self)

    def get_context(self) -> dict:
        """Override context retrieval."""
        return {}

    class Collection(ArchCollection["GenericEdge"]):
        """Default Generic Collection."""

        __collection__ = "edge"

        @classmethod
        def __document__(cls, doc: Mapping[str, Any]) -> "GenericEdge":
            """Return parsed EdgeArchitype from document."""
            return cls.build_edge(
                DocAnchor[GenericEdge](
                    type=JType.edge,
                    id=cast(ObjectId, doc.get("_id")),
                    root=cast(ObjectId, doc.get("root")),
                    access=DocAccess.from_json(cast(dict, doc.get("access"))),
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

    def __init__(
        self,
        request: Optional[Request] = None,
        entry: Optional[str] = None,
        save_on_exit: bool = True,
    ) -> None:
        """Create JacContext."""
        self.__mem__: dict[ObjectId, Union[NodeArchitype, EdgeArchitype]] = {}
        self.__del__: dict[ObjectId, Union[NodeArchitype, EdgeArchitype]] = {}
        self.request = request
        self.user = getattr(request, "auth_user", None)
        self.root: Root = cast(Root, getattr(request, "auth_root", base_root))
        self.reports: list[Any] = []
        self.entry = entry
        self.save_on_exit: bool = save_on_exit

    @staticmethod
    def get_context() -> "JacContext":
        """Get or create JacContext."""
        if not (jctx := JCONTEXT.get(None)):
            JCONTEXT.set(jctx := JacContext())
        return jctx

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
                if isinstance(entry, NodeArchitype) and await self.root.is_allowed(
                    entry
                ):
                    self.entry = entry
                else:
                    raise HTTPException(status_code=403)
            else:
                self.entry = self.root
        elif self.entry is None:
            self.entry = self.root

        if isinstance(self.entry, NodeArchitype):
            JacContext.try_set(self.entry)

        return self.entry

    async def check_root_access(
        self, root_id: Optional[ObjectId] = None
    ) -> Optional[DocAccess]:
        """Retrieve current root or specified root."""
        if root_id:
            if (obj := self.get(root_id)) and isinstance(obj, Root):
                return obj._jac_doc_.access

            if isinstance(root := await Root.Collection.find_by_id(root_id), Root):
                self.set(root._jac_doc_.id, root)
                return root._jac_doc_.access
        elif isinstance(self.root, Root):
            return self.root._jac_doc_.access
        return None

    async def populate_edges(
        self, danchors: list[Union[EdgeArchitype, DocAnchor[EdgeArchitype]]]
    ) -> None:
        """Populate in-memory edges references."""
        queue = {
            did: danchor.class_ref()
            for danchor in danchors
            if isinstance(danchor, DocAnchor) and not self.has(did := danchor.id)
        }

        nodes: list[Union[NodeArchitype, DocAnchor[NodeArchitype]]] = []
        # allowed to throw error to stop executions
        async for edge in EdgeArchitype.Collection.collection().find(
            {"_id": {"$in": list(queue.keys())}}
        ):
            if cls := queue.get(edge["_id"]):
                arch = cls.Collection.__document__(edge)
                self.set(arch._jac_doc_.id, arch)

                jac = arch._jac_
                if jac.source and jac.target:
                    nodes.append(jac.source)
                    nodes.append(jac.target)

        await self.populate_nodes(nodes)

    async def populate_nodes(
        self, danchors: list[Union[NodeArchitype, DocAnchor[NodeArchitype]]]
    ) -> None:
        """Populate in-memory nodes references."""
        queue = {
            did: danchor.class_ref()
            for danchor in danchors
            if isinstance(danchor, DocAnchor) and not self.has(did := danchor.id)
        }

        # allowed to throw error to stop executions
        async for edge in NodeArchitype.Collection.collection().find(
            {"_id": {"$in": list(queue.keys())}}
        ):
            if cls := queue.get(edge["_id"]):
                arch = cls.Collection.__document__(edge)
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
        self, id: Union[ObjectId, str], obj: Union[NodeArchitype, EdgeArchitype]
    ) -> None:
        """Push Architype in memory via ID."""
        self.__mem__[ObjectId(id)] = obj

    @staticmethod
    def try_set(obj: Union[NodeArchitype, EdgeArchitype]) -> None:
        """Try to push Architype in memory via ID."""
        if isinstance(jctx := JCONTEXT.get(None), JacContext):
            jctx.set(obj._jac_doc_.id, obj)

    def remove(
        self, id: Union[ObjectId, str]
    ) -> Optional[Union["NodeArchitype", "EdgeArchitype"]]:
        """Pull Architype in memory via ID."""
        return self.__mem__.pop(ObjectId(id), None)

    def destroy(
        self, id: Union[ObjectId, str], obj: Union[NodeArchitype, EdgeArchitype]
    ) -> None:
        """Push Architype in memory via ID."""
        self.__del__[ObjectId(id)] = obj

    @staticmethod
    def try_destroy(obj: Union[NodeArchitype, EdgeArchitype]) -> None:
        """Push Architype in memory via ID."""
        if isinstance(jctx := JCONTEXT.get(None), JacContext):
            jctx.destroy(obj._jac_doc_.id, obj)

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

        if not SHOW_ENDPOINT_RETURNS:
            resp.pop("returns")

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

    def no_auto_save(self) -> None:
        """Disable automatic saving on exit."""
        self.save_on_exit = False

    async def clean_up(self) -> None:
        """Save all architypes."""
        if self.save_on_exit:
            self.save_on_exit = False
            async with await ArchCollection.get_session() as session:
                async with session.start_transaction():
                    for arch in self.__del__.values():
                        await arch._destroy(session)
                    for arch in self.__mem__.values():
                        await arch._save(session)
                    await session.commit_transaction()
                    await commit_session_with_retry(session)


async def async_filter(
    edges: list[Union["EdgeArchitype", DocAnchor["EdgeArchitype"]]]
) -> AsyncGenerator[tuple[EdgeArchitype, NodeArchitype, NodeArchitype], None]:
    """Async filter for edges."""
    for edge in edges:
        if (
            (ed := await edge._connect())
            and (edj := ed._jac_)
            and (edjs := edj.source)
            and (edjt := edj.target)
            and (src := await edjs._connect())
            and (tgt := await edjt._connect())
        ):
            yield (ed, src, tgt)


Root.__name__ = ""
GenericEdge.__name__ = ""
NodeCollection = NodeArchitype.Collection
EdgeCollection = EdgeArchitype.Collection

JCLASS: dict[str, dict[str, type]] = {"n": {"": Root}, "e": {"": GenericEdge}}

base_root_id = ObjectId("000000000000000000000000")
base_root = Root.Collection.build_node(
    DocAnchor[Root](
        type=JType.node,
        id=base_root_id,
        root=base_root_id,
        access=DocAccess(),
        connected=True,
    ),
    {"context": {}, "edge": []},
)
