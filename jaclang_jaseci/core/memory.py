"""Memory abstraction for jaseci plugin."""

from dataclasses import dataclass, field
from typing import (
    AsyncGenerator,
    Callable,
    Generator,
    Optional,
    Type,
    Union,
)

from bson import ObjectId

# from jaclang.core.architype import MANUAL_SAVE

from motor.motor_asyncio import AsyncIOMotorClientSession

from .architype import (
    Anchor,
    AnchorType,
    EdgeAnchor,
    NodeAnchor,
    WalkerAnchor,
)

IDS = Union[ObjectId, list[ObjectId]]


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

    __session__: Optional[AsyncIOMotorClientSession] = None

    async def find(  # type: ignore[override]
        self,
        type: AnchorType,
        ids: IDS,
        filter: Optional[Callable[[Anchor], Anchor]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> AsyncGenerator[Anchor, None]:
        """Find anchors from datasource by ids with filter."""
        if not isinstance(ids, list):
            ids = [ids]

        base_anchor: Type[Anchor] = Anchor
        match type:
            case AnchorType.node:
                base_anchor = NodeAnchor
            case AnchorType.edge:
                base_anchor = EdgeAnchor
            case AnchorType.walker:
                base_anchor = WalkerAnchor
            case _:
                pass

        async for anchor in await base_anchor.Collection.find(
            {
                "_id": {"$in": [id for id in ids if id not in self.__mem__]},
            },
            session=session or self.__session__,
        ):
            self.__mem__[anchor.id] = anchor

        for id in ids:
            if (_anchor := self.__mem__.get(id)) and (not filter or filter(_anchor)):
                yield _anchor

    async def find_one(  # type: ignore[override]
        self,
        type: AnchorType,
        ids: IDS,
        filter: Optional[Callable[[Anchor], Anchor]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> Optional[Anchor]:
        """Find one anchor from memory by ids with filter."""
        return await anext(self.find(type, ids, filter, session), None)

    def remove(self, data: Union[Anchor, list[Anchor]]) -> None:
        """Remove anchor/s from datasource."""
        super().remove(data)
