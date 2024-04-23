"""Graph Docs Plugin."""

from dataclasses import fields
from typing import Callable, Optional, Type, Union

from jaclang.core.construct import Architype, DSFunc, EdgeDir
from jaclang.plugin.default import hookimpl
from jaclang.plugin.feature import JacFeature as Jac

from .common import (
    ArchCollection,
    EdgeArchitype,
    GenericEdge,
    JCLASS,
    JCONTEXT,
    JType,
    JacContext,
    NodeArchitype,
    async_filter,
)


class JacPlugin:
    """Plugin Methods."""

    @staticmethod
    @hookimpl
    def make_node(
        on_entry: list[DSFunc], on_exit: list[DSFunc]
    ) -> Callable[[type], type]:
        """Create a obj architype."""

        def decorator(cls: Type[Architype]) -> Type[Architype]:
            """Decorate class."""
            cls = Jac.make_architype(
                cls, arch_base=NodeArchitype, on_entry=on_entry, on_exit=on_exit
            )
            populate_collection(cls, JType.node)
            return cls

        return decorator

    @staticmethod
    @hookimpl
    def make_edge(
        on_entry: list[DSFunc], on_exit: list[DSFunc]
    ) -> Callable[[type], type]:
        """Create a edge architype."""

        def decorator(cls: Type[Architype]) -> Type[Architype]:
            """Decorate class."""
            cls = Jac.make_architype(
                cls, arch_base=EdgeArchitype, on_entry=on_entry, on_exit=on_exit
            )
            populate_collection(cls, JType.edge)
            return cls

        return decorator

    @staticmethod
    @hookimpl
    async def edge_ref(
        node_obj: NodeArchitype | list[NodeArchitype],
        target_obj: Optional[NodeArchitype | list[NodeArchitype]],
        dir: EdgeDir,
        filter_func: Optional[Callable[[list[EdgeArchitype]], list[EdgeArchitype]]],
        edges_only: bool,
    ) -> list[NodeArchitype] | list[EdgeArchitype]:
        """Jac's apply_dir stmt feature."""
        if isinstance(node_obj, NodeArchitype):
            node_obj = [node_obj]
        targ_obj_set: Optional[list[NodeArchitype]] = (
            (target_obj if isinstance(target_obj, list) else [target_obj])
            if target_obj
            else None
        )
        if edges_only:
            connected_edges: list[EdgeArchitype] = []
            for node in node_obj:
                connected_edges += await node._jac_.get_edges(
                    dir, filter_func, target_obj=targ_obj_set
                )
            return list(set(connected_edges))
        else:
            connected_nodes: list[NodeArchitype] = []
            for node in node_obj:
                connected_nodes.extend(
                    await node._jac_.edges_to_nodes(
                        dir, filter_func, target_obj=targ_obj_set
                    )
                )
            return list(set(connected_nodes))

    @staticmethod
    @hookimpl
    def build_edge(
        is_undirected: bool,
        conn_type: Optional[Type[EdgeArchitype]],
        conn_assign: Optional[tuple[tuple, tuple]],
    ) -> Callable[[], EdgeArchitype]:
        """Jac's root getter."""
        conn_type = conn_type if conn_type else GenericEdge

        def builder() -> EdgeArchitype:
            edge = conn_type()
            edge._jac_.is_undirected = is_undirected
            if conn_assign:
                for fld, val in zip(conn_assign[0], conn_assign[1]):
                    if hasattr(edge, fld):
                        setattr(edge, fld, val)
                    else:
                        raise ValueError(f"Invalid attribute: {fld}")
            return edge

        return builder

    @staticmethod
    @hookimpl
    async def disconnect(
        left: NodeArchitype | list[NodeArchitype],
        right: NodeArchitype | list[NodeArchitype],
        dir: EdgeDir,
        filter_func: Optional[Callable[[list[EdgeArchitype]], list[EdgeArchitype]]],
    ) -> bool:  # noqa: ANN401
        """Jac's disconnect operator feature."""
        disconnect_occurred = False
        left = [left] if isinstance(left, NodeArchitype) else left
        right = [right] if isinstance(right, NodeArchitype) else right

        jctx: JacContext = JCONTEXT.get()
        await jctx.populate_edges([edge for node in left for edge in node._jac_.edges])

        for i in left:
            async for e, s, t in async_filter(i._jac_.edges):
                if not filter_func or filter_func([e]):
                    if (
                        dir in [EdgeDir.OUT, EdgeDir.ANY]
                        and i == s
                        and t in right
                        and await s.is_allowed(t, jctx)
                    ):
                        await e.destroy()
                        disconnect_occurred = True
                    if (
                        dir in [EdgeDir.IN, EdgeDir.ANY]
                        and i == t
                        and s in right
                        and await t.is_allowed(s, jctx)
                    ):
                        await e.destroy()
                        disconnect_occurred = True
        return disconnect_occurred


def populate_collection(
    cls: Type[Union[NodeArchitype, EdgeArchitype]], jtype: JType
) -> type:
    """Override Architype's Collection to support MongoDB operations."""
    cls_name = cls.__name__
    JCLASS[jtype.value][cls_name] = cls

    cls._jac_fields_ = [field.name for field in fields(cls)]

    collection = f"{jtype.name.lower()}"
    if (coll := getattr(cls, "Collection", None)) is None:

        class Collection(ArchCollection):
            __collection__ = collection
            __excluded__ = []  # private fields
            __indexes__ = []  # not sure yet

        cls.Collection = Collection  # type: ignore
    elif not issubclass(coll, ArchCollection):
        cls.Collection = type(  # type: ignore
            coll.__name__, (coll, ArchCollection), {"__collection__": collection}
        )
    else:
        cls.Collection = type(coll.__name__, (coll,), {})  # type: ignore

    return cls