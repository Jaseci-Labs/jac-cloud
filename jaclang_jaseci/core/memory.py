"""Memory abstraction for jaseci plugin."""

from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Optional, Union, cast

from bson import ObjectId

from jaclang.core.architype import MANUAL_SAVE

from .architype import (
    Anchor,
    Architype,
    EdgeAnchor,
    EdgeArchitype,
    NodeAnchor,
    NodeArchitype,
    ObjectType,
    Permission,
    WalkerAnchor,
    WalkerArchitype,
)

IDS = Union[ObjectId, list[ObjectId]]
ANCS = Union[Anchor, list[Anchor]]


@dataclass
class Memory:
    """Generic Memory Handler."""

    __mem__: dict[ObjectId, Anchor] = field(default_factory=dict)
    __gc__: set[ObjectId] = field(default_factory=set)

    def close(self) -> None:
        """Close memory handler."""
        self.__mem__.clear()
        self.__gc__.clear()

    def __del__(self) -> None:
        """On garbage collection cleanup."""
        self.close()

    def find(
        self, ids: IDS, filter: Optional[Callable[[Anchor], Anchor]] = None
    ) -> Generator[Anchor, None, None]:
        """Find anchors from memory by ids with filter."""
        if not isinstance(ids, list):
            ids = [ids]

        return (
            anchor
            for id in ids
            if (anchor := self.__mem__.get(id)) and (not filter or filter(anchor))
        )

    def find_one(
        self,
        ids: IDS,
        filter: Optional[Callable[[Anchor], Anchor]] = None,
    ) -> Optional[Anchor]:
        """Find one anchor from memory by ids with filter."""
        return next(self.find(ids, filter), None)

    def set(self, data: Union[Anchor, list[Anchor]]) -> None:
        """Save anchor/s to memory."""
        if isinstance(data, list):
            for d in data:
                if d.id not in self.__gc__:
                    self.__mem__[d.id] = d
        elif data.id not in self.__gc__:
            self.__mem__[data.id] = data

    def remove(self, data: Union[Anchor, list[Anchor]]) -> None:
        """Remove anchor/s from memory."""
        if isinstance(data, list):
            for d in data:
                self.__mem__.pop(d.id, None)
                self.__gc__.add(d.id)
        else:
            self.__mem__.pop(data.id, None)
            self.__gc__.add(data.id)


@dataclass
class MongoDB(Memory):
    """Shelf Handler."""

    def find(  # type: ignore[override]
        self, anchors: ANCS, filter: Optional[Callable[[Anchor], Anchor]] = None
    ) -> Generator[Anchor, None, None]:
        """Find anchors from datasource by ids with filter."""
        if not isinstance(anchors, list):
            anchors = [anchors]

        for anchor in anchors:
            _anchor = self.__mem__.get(anchor.id)
            MANUAL_SAVE

            if _anchor and (not filter or filter(_anchor)):
                yield _anchor

    def set(self, data: Union[Anchor, list[Anchor]], mem_only: bool = False) -> None:
        """Save anchor/s to datasource."""
        super().set(data)

    def remove(self, data: Union[Anchor, list[Anchor]]) -> None:
        """Remove anchor/s from datasource."""
        super().remove(data)

    def get(self, anchor: dict[str, Any]) -> Anchor:
        """Get Anchor Instance."""
        name = cast(str, anchor.get("name"))
        architype = anchor.pop("architype")
        access = Permission.deserialize(anchor.pop("access"))

        match ObjectType(anchor.pop("type")):
            case ObjectType.node:
                nanch = NodeAnchor(
                    edges=[
                        e for edge in anchor.pop("edges") if (e := EdgeAnchor.ref(edge))
                    ],
                    access=access,
                    connected=True,
                    **anchor,
                )
                nanch.architype = NodeArchitype.get(name or "Root")(
                    __jac__=nanch, **architype
                )
                return nanch
            case ObjectType.edge:
                eanch = EdgeAnchor(
                    source=NodeAnchor.ref(anchor.pop("source")),
                    target=NodeAnchor.ref(anchor.pop("target")),
                    access=access,
                    connected=True,
                    **anchor,
                )
                eanch.architype = EdgeArchitype.get(name or "GenericEdge")(
                    __jac__=eanch, **architype
                )
                return eanch
            case ObjectType.walker:
                wanch = WalkerAnchor(access=access, connected=True, **anchor)
                wanch.architype = WalkerArchitype.get(name)(__jac__=wanch, **architype)
                return wanch
            case _:
                oanch = Anchor(access=access, connected=True, **anchor)
                oanch.architype = Architype(__jac__=oanch)
                return oanch
