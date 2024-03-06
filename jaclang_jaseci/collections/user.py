"""UserCollection Interface."""

from typing import Optional

from .base import BaseCollection


class UserCollection(BaseCollection):
    """
    User collection interface.

    This interface is for User's Management.
    You may override this if you wish to implement different structure
    """

    __collection__: Optional[str] = "user"
    __excluded__: list[str] = ["password"]
    __indexes__: list[dict] = [{"fields": ["email"], "unique": True}]

    @classmethod
    async def find_by_email(cls, email: str) -> object:
        """Retrieve user via email."""
        return await cls.find_one(filter={"email": email}, projection=None)
