"""BaseCollection Interface."""

from os import getenv
from typing import Any, AsyncGenerator, Awaitable, Callable, Optional, Union, cast

from bson import ObjectId

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
    AsyncIOMotorCursor,
    AsyncIOMotorDatabase,
)

from pymongo import (
    DeleteMany,
    DeleteOne,
    IndexModel,
    InsertOne,
    MongoClient,
    UpdateMany,
    UpdateOne,
)
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.server_api import ServerApi

from ..utils import logger


class BaseCollection:
    """
    Base collection interface.

    This interface use for connecting to mongodb.
    """

    __collection__: Optional[str] = None
    __collection_obj__: Optional[AsyncIOMotorCollection] = None  # type: ignore[valid-type]
    __indexes__: list = []
    __excluded__: list = []
    __excluded_obj__ = None

    __client__: Optional[AsyncIOMotorClient] = None  # type: ignore[valid-type]
    __database__: Optional[AsyncIOMotorDatabase] = None  # type: ignore[valid-type]

    @classmethod
    def __document__(cls, doc: dict) -> Union[dict, object]:
        """
        Return parsed version of document.

        This the default parser after getting a single document.
        You may override this to specify how/which class it will be casted/based.
        """
        return doc

    @classmethod
    async def __documents__(
        cls, docs: AsyncIOMotorCursor  # type: ignore[valid-type]
    ) -> AsyncGenerator[Union[dict, object], None]:
        """
        Return parsed version of multiple documents.

        This the default parser after getting a list of documents.
        You may override this to specify how/which class it will be casted/based.
        """
        return (cls.__document__(doc) async for doc in docs)  # type: ignore[attr-defined]

    @staticmethod
    def get_client() -> AsyncIOMotorClient:  # type: ignore[valid-type]
        """Return pymongo.database.Database for mongodb connection."""
        if not isinstance(BaseCollection.__client__, AsyncIOMotorClient):
            BaseCollection.__client__ = AsyncIOMotorClient(
                getenv(
                    "DATABASE_HOST",
                    "mongodb://localhost/?retryWrites=true&w=majority",
                ),
                server_api=ServerApi("1"),
            )

        return BaseCollection.__client__

    @staticmethod
    async def get_session() -> AsyncIOMotorClientSession:  # type: ignore[valid-type]
        """Return pymongo.client_session.ClientSession used for mongodb transactional operations."""
        return await cast(MongoClient, BaseCollection.get_client()).start_session()  # type: ignore[misc]

    @staticmethod
    def get_database() -> AsyncIOMotorDatabase:  # type: ignore[valid-type]
        """Return pymongo.database.Database for database connection based from current client connection."""
        if not isinstance(BaseCollection.__database__, AsyncIOMotorDatabase):
            BaseCollection.__database__ = cast(
                MongoClient, BaseCollection.get_client()
            ).get_database(getenv("DATABASE_NAME", "jaclang"))

        return BaseCollection.__database__

    @staticmethod
    def get_collection(collection: str) -> AsyncIOMotorCollection:  # type: ignore[valid-type]
        """Return pymongo.collection.Collection for collection connection based from current database connection."""
        return cast(Database, BaseCollection.get_database()).get_collection(collection)

    @classmethod
    async def collection(
        cls, session: Optional[AsyncIOMotorClientSession] = None  # type: ignore[valid-type]
    ) -> AsyncIOMotorCollection:  # type: ignore[valid-type]
        """Return pymongo.collection.Collection for collection connection based from attribute of it's child class."""
        if not isinstance(cls.__collection_obj__, AsyncIOMotorCollection):
            cls.__collection_obj__ = cls.get_collection(
                getattr(cls, "__collection__", None) or cls.__name__.lower()
            )

            if cls.__excluded__:
                excl_obj = {}
                for excl in cls.__excluded__:
                    excl_obj[excl] = False
                cls.__excluded_obj__ = excl_obj

            if cls.__indexes__:
                idxs = []
                while cls.__indexes__:
                    idx = cls.__indexes__.pop()
                    idxs.append(IndexModel(idx.pop("fields"), **idx))

                ops = cast(Collection, cls.__collection_obj__).create_indexes(
                    idxs, session=session
                )
                if isinstance(ops, Awaitable):
                    await ops

        return cls.__collection_obj__

    @classmethod
    async def insert_one(
        cls, doc: dict, session: Optional[AsyncIOMotorClientSession] = None  # type: ignore[valid-type]
    ) -> Optional[ObjectId]:
        """Insert single document and return the inserted id."""
        try:
            collection = await cls.collection(session=session)

            ops = cast(Collection, collection).insert_one(doc, session=session)
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops.inserted_id
        except Exception:
            if session:
                raise
            logger.exception(f"Error inserting doc:\n{doc}")
        return None

    @classmethod
    async def insert_many(
        cls, docs: list[dict], session: Optional[AsyncIOMotorClientSession] = None  # type: ignore[valid-type]
    ) -> list[ObjectId]:
        """Insert multiple documents and return the inserted ids."""
        try:
            collection = await cls.collection(session=session)

            ops = cast(Collection, collection).insert_many(docs, session=session)
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops.inserted_ids
        except Exception:
            if session:
                raise
            logger.exception(f"Error inserting docs:\n{docs}")
        return []

    @classmethod
    async def update_one(
        cls,
        filter: dict,
        update: dict,
        session: Optional[AsyncIOMotorClientSession] = None,  # type: ignore[valid-type]
    ) -> int:
        """Update single document and return if it's modified or not."""
        try:
            collection = await cls.collection(session=session)

            ops = cast(Collection, collection).update_one(
                filter, update, session=session
            )
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops.modified_count
        except Exception:
            if session:
                raise
            logger.exception(f"Error updating doc:\n{filter}\n{update}")
        return 0

    @classmethod
    async def update_many(
        cls,
        filter: dict,
        update: dict,
        session: Optional[AsyncIOMotorClientSession] = None,  # type: ignore[valid-type]
    ) -> int:
        """Update multiple documents and return how many docs are modified."""
        try:
            collection = await cls.collection(session=session)

            ops = cast(Collection, collection).update_many(
                filter, update, session=session
            )
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops.modified_count
        except Exception:
            if session:
                raise
            logger.exception(f"Error updating doc:\n{filter}\n{update}")
        return 0

    @classmethod
    async def update_by_id(
        cls,
        id: Union[str, ObjectId],
        update: dict,
        session: Optional[AsyncIOMotorClientSession] = None,  # type: ignore[valid-type]
    ) -> int:
        """Update single document via ID and return if it's modified or not."""
        return await cls.update_one({"_id": ObjectId(id)}, update, session)

    @classmethod
    async def find(
        cls,
        *args: list[Any],
        cursor: Callable = lambda x: x,
        **kwargs: Any,  # noqa ANN401
    ) -> AsyncGenerator[Union[dict, object], None]:
        """Retrieve multiple documents."""
        collection = await cls.collection()

        if "projection" not in kwargs:
            kwargs["projection"] = cls.__excluded_obj__

        docs = cursor(cast(Collection, collection).find(*args, **kwargs))
        return await cls.__documents__(docs)

    @classmethod
    async def find_one(cls, *args: Any, **kwargs: Any) -> object:  # noqa ANN401
        """Retrieve single document from db."""
        collection = await cls.collection()

        if "projection" not in kwargs:
            kwargs["projection"] = cls.__excluded_obj__

        ops = cast(Collection, collection).find_one(*args, **kwargs)
        if isinstance(ops, Awaitable):
            ops = await ops

        if ops:
            return cls.__document__(ops)
        return ops

    @classmethod
    async def find_by_id(
        cls, id: Union[str, ObjectId], *args: list[Any], **kwargs: dict[str, Any]
    ) -> object:
        """Retrieve single document via ID."""
        return await cls.find_one({"_id": ObjectId(id)}, *args, **kwargs)

    @classmethod
    async def delete(
        cls, filter: dict, session: Optional[AsyncIOMotorClientSession] = None  # type: ignore[valid-type]
    ) -> int:
        """Delete document/s via filter and return how many documents are deleted."""
        try:
            collection = await cls.collection(session=session)

            ops = cast(Collection, collection).delete_many(filter, session=session)
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops.deleted_count
        except Exception:
            if session:
                raise
            logger.exception(f"Error delete with filter:\n{filter}")
        return 0

    @classmethod
    async def delete_one(
        cls, filter: dict, session: Optional[AsyncIOMotorClientSession] = None  # type: ignore[valid-type]
    ) -> int:
        """Delete single document via filter and return if it's deleted or not."""
        try:
            collection = await cls.collection(session=session)

            ops = cast(Collection, collection).delete_one(filter, session=session)
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops.deleted_count
        except Exception:
            if session:
                raise
            logger.exception(f"Error delete one with filter:\n{filter}")
        return 0

    @classmethod
    async def delete_by_id(
        cls,
        id: Union[str, ObjectId],
        session: Optional[AsyncIOMotorClientSession] = None,  # type: ignore[valid-type]
    ) -> int:
        """Delete single document via ID and return if it's deleted or not."""
        return await cls.delete_one({"_id": ObjectId(id)}, session)

    @classmethod
    async def bulk_write(
        cls,
        ops: list[Union[InsertOne, DeleteMany, DeleteOne, UpdateMany, UpdateOne]],
        session: Optional[AsyncIOMotorClientSession] = None,  # type: ignore[valid-type]
    ) -> dict:  # noqa ANN401
        """Bulk write operations."""
        try:
            collection = await cls.collection(session=session)

            _ops = cast(Collection, collection).bulk_write(ops, session=session)
            if isinstance(_ops, Awaitable):
                _ops = await _ops

            return _ops.bulk_api_result
        except Exception:
            if session:
                raise
            logger.exception(f"Error bulk write:\n{ops}")
        return {}
