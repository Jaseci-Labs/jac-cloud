"""CodeMemory Interface."""

from .common import CommonMemory


class CodeMemory(CommonMemory):
    """Code Memory Interface.

    This interface is for Code Management such as Verification Code.
    You may override this if you wish to implement different structure
    """

    __table__ = "verification"
