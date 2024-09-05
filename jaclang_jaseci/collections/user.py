"""UserCollection Interface."""

from typing import Any, Mapping, Optional, TypeVar, Union

from .base import BaseCollection

T = TypeVar("T")


class UserCollection(BaseCollection[T]):
    """
    User collection interface.

    This interface is for User's Management.
    You may override this if you wish to implement different structure
    """

    __collection__: Optional[str] = "user"
    __excluded__: list[str] = ["password"]
    __indexes__: list[dict] = [{"keys": ["email"], "unique": True}]

    @classmethod
    async def find_by_email(cls, email: str) -> Optional[Union[T, Mapping[str, Any]]]:
        """Retrieve user via email."""
        return await cls.find_one(filter={"email": email}, projection={})
