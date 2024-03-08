"""BaseCollection Interface."""

from os import getenv
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Iterable,
    Mapping,
    Optional,
    Union,
)

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
    UpdateMany,
    UpdateOne,
)
from pymongo.server_api import ServerApi

from ..utils import logger


class BaseCollection:
    """
    Base collection interface.

    This interface use for connecting to mongodb.
    """

    __collection__: Optional[str] = None
    __collection_obj__: Optional[AsyncIOMotorCollection] = None
    __indexes__: list = []
    __excluded__: list = []
    __excluded_obj__: Optional[Mapping[str, Any]] = None

    __client__: Optional[AsyncIOMotorClient] = None
    __database__: Optional[AsyncIOMotorDatabase] = None

    @classmethod
    def __document__(
        cls, doc: Union[Mapping[str, Any], dict[str, Any]]
    ) -> Union[dict, object]:
        """
        Return parsed version of document.

        This the default parser after getting a single document.
        You may override this to specify how/which class it will be casted/based.
        """
        return doc

    @classmethod
    async def __documents__(
        cls, docs: AsyncIOMotorCursor
    ) -> AsyncGenerator[Union[dict, object], None]:
        """
        Return parsed version of multiple documents.

        This the default parser after getting a list of documents.
        You may override this to specify how/which class it will be casted/based.
        """
        return (cls.__document__(doc) async for doc in docs)

    @staticmethod
    def get_client() -> AsyncIOMotorClient:
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
    async def get_session() -> AsyncIOMotorClientSession:
        """Return pymongo.client_session.ClientSession used for mongodb transactional operations."""
        return await BaseCollection.get_client().start_session()

    @staticmethod
    def get_database() -> AsyncIOMotorDatabase:
        """Return pymongo.database.Database for database connection based from current client connection."""
        if not isinstance(BaseCollection.__database__, AsyncIOMotorDatabase):
            BaseCollection.__database__ = BaseCollection.get_client().get_database(
                getenv("DATABASE_NAME", "jaclang")
            )

        return BaseCollection.__database__

    @staticmethod
    def get_collection(collection: str) -> AsyncIOMotorCollection:
        """Return pymongo.collection.Collection for collection connection based from current database connection."""
        return BaseCollection.get_database().get_collection(collection)

    @classmethod
    async def collection(
        cls, session: Optional[AsyncIOMotorClientSession] = None
    ) -> AsyncIOMotorCollection:
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

                ops = cls.__collection_obj__.create_indexes(idxs, session=session)
                if isinstance(ops, Awaitable):
                    await ops

        return cls.__collection_obj__

    @classmethod
    async def insert_one(
        cls, doc: dict, session: Optional[AsyncIOMotorClientSession] = None
    ) -> Optional[ObjectId]:
        """Insert single document and return the inserted id."""
        try:
            collection = await cls.collection(session=session)

            ops = await collection.insert_one(doc, session=session)
            return ops.inserted_id
        except Exception:
            if session:
                raise
            logger.exception(f"Error inserting doc:\n{doc}")
        return None

    @classmethod
    async def insert_many(
        cls, docs: list[dict], session: Optional[AsyncIOMotorClientSession] = None
    ) -> list[ObjectId]:
        """Insert multiple documents and return the inserted ids."""
        try:
            collection = await cls.collection(session=session)

            ops = await collection.insert_many(docs, session=session)
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
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> int:
        """Update single document and return if it's modified or not."""
        try:
            collection = await cls.collection(session=session)

            ops = await collection.update_one(filter, update, session=session)
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
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> int:
        """Update multiple documents and return how many docs are modified."""
        try:
            collection = await cls.collection(session=session)

            ops = await collection.update_many(filter, update, session=session)
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
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> int:
        """Update single document via ID and return if it's modified or not."""
        return await cls.update_one({"_id": ObjectId(id)}, update, session)

    @classmethod
    async def find(
        cls,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> AsyncGenerator[Union[dict, object], None]:
        """Retrieve multiple documents."""
        collection = await cls.collection()

        if projection is None:
            projection = cls.__excluded_obj__

        docs = collection.find(filter, projection, session=session, **kwargs)
        return await cls.__documents__(docs)

    @classmethod
    async def find_one(
        cls,
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> object:
        """Retrieve single document from db."""
        collection = await cls.collection()

        if projection is None:
            projection = cls.__excluded_obj__

        if ops := await collection.find_one(
            filter, projection, session=session, **kwargs
        ):
            return cls.__document__(ops)
        return ops

    @classmethod
    async def find_by_id(
        cls,
        id: Union[str, ObjectId],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        session: Optional[AsyncIOMotorClientSession] = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> object:
        """Retrieve single document via ID."""
        return await cls.find_one(
            {"_id": ObjectId(id)}, projection, session=session, **kwargs
        )

    @classmethod
    async def delete(
        cls, filter: dict, session: Optional[AsyncIOMotorClientSession] = None
    ) -> int:
        """Delete document/s via filter and return how many documents are deleted."""
        try:
            collection = await cls.collection(session=session)

            ops = await collection.delete_many(filter, session=session)
            return ops.deleted_count
        except Exception:
            if session:
                raise
            logger.exception(f"Error delete with filter:\n{filter}")
        return 0

    @classmethod
    async def delete_one(
        cls, filter: dict, session: Optional[AsyncIOMotorClientSession] = None
    ) -> int:
        """Delete single document via filter and return if it's deleted or not."""
        try:
            collection = await cls.collection(session=session)

            ops = await collection.delete_one(filter, session=session)
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
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> int:
        """Delete single document via ID and return if it's deleted or not."""
        return await cls.delete_one({"_id": ObjectId(id)}, session)

    @classmethod
    async def bulk_write(
        cls,
        ops: list[Union[InsertOne, DeleteMany, DeleteOne, UpdateMany, UpdateOne]],
        session: Optional[AsyncIOMotorClientSession] = None,
    ) -> dict:
        """Bulk write operations."""
        try:
            collection = await cls.collection(session=session)

            _ops = await collection.bulk_write(ops, session=session)
            return _ops.bulk_api_result
        except Exception:
            if session:
                raise
            logger.exception(f"Error bulk write:\n{ops}")
        return {}
