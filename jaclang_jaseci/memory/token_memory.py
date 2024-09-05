"""TokenMemory Interface."""

from .base import BaseMemory


class TokenMemory(BaseMemory):
    """Token Memory Interface.

    This interface is for Token Management.
    You may override this if you wish to implement different structure
    """

    __table__ = "token"
