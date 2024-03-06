"""Memory Interface."""

from os import getenv
from typing import Any, Awaitable, Optional

from orjson import dumps, loads

from redis import Redis, asyncio as aioredis

from ..utils import logger


class CommonMemory:
    """
    Base Memory interface.

    This interface use for connecting to redis.
    """

    __table__ = "common"
    __redis__: Optional[Redis] = None

    @staticmethod
    def get_rd() -> Redis:
        """Return redis.Redis for Redis connection."""
        if not isinstance(CommonMemory.__redis__, Redis):
            CommonMemory.__redis__ = aioredis.from_url(
                getenv("REDIS_HOST", "redis://localhost"),
                port=int(getenv("REDIS_PORT", "6379")),
                username=getenv("REDIS_USER"),
                password=getenv("REDIS_PASS"),
            )
        return CommonMemory.__redis__

    @classmethod
    async def get(cls, key: str) -> Any:  # noqa: ANN401
        """Retrieve via key."""
        try:
            redis = cls.get_rd()
            return loads(await redis.get(key))
        except Exception:
            logger.exception(f"Error getting key {key}")
            return None

    @classmethod
    async def keys(cls) -> list[str]:
        """Return all available keys."""
        try:
            redis = cls.get_rd()
            return await redis.keys()
        except Exception:
            logger.exception("Error getting keys")
            return []

    @classmethod
    async def set(cls, key: str, data: Any) -> bool:  # noqa: ANN401
        """Push key value pair."""
        try:
            redis = cls.get_rd()
            return bool(await redis.set(key, dumps(data)))
        except Exception:
            logger.exception(f"Error setting key {key} with data\n{data}")
            return False

    @classmethod
    async def delete(cls, key: str) -> bool:
        """Delete via key."""
        try:
            redis = cls.get_rd()
            return bool(await redis.delete(key))
        except Exception:
            logger.exception(f"Error deleting key {key}")
            return False

    @classmethod
    async def hget(cls, key: str) -> Any:  # noqa: ANN401
        """Retrieve via key from group."""
        try:
            redis = cls.get_rd()

            ops = redis.hget(cls.__table__, key)
            if isinstance(ops, Awaitable):
                ops = await ops

            if ops is not None:
                return loads(ops)
        except Exception:
            logger.exception(f"Error getting key {key} from {cls.__table__}")

    @classmethod
    async def hkeys(cls) -> list[str]:
        """Retrieve all available keys from group."""
        try:
            redis = cls.get_rd()

            ops = redis.hkeys(cls.__table__)
            if isinstance(ops, Awaitable):
                ops = await ops

            return ops
        except Exception:
            logger.exception(f"Error getting keys from {cls.__table__}")
            return []

    @classmethod
    async def hset(cls, key: str, data: dict) -> bool:
        """Push key value pair to group."""
        try:
            redis = cls.get_rd()

            ops = redis.hset(cls.__table__, key, dumps(data).decode())
            if isinstance(ops, Awaitable):
                ops = await ops

            return bool(ops)
        except Exception:
            logger.exception(
                f"Error setting key {key} from {cls.__table__} with data\n{data}"
            )
            return False

    @classmethod
    async def hdelete(cls, *keys: Any) -> bool:  # noqa ANN401
        """Delete via key from group."""
        try:
            redis = cls.get_rd()

            ops = redis.hdel(cls.__table__, *keys)
            if isinstance(ops, Awaitable):
                ops = await ops

            return bool(ops)
        except Exception:
            logger.exception(f"Error deleting key {keys} from {cls.__table__}")
            return False
