"""Common Utilities."""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, cast, get_args, get_origin

from .mail import Emailer, SendGridEmailer


def utc_datetime(**addons: int) -> datetime:
    """Get current datetime with option to add additional timedelta."""
    return datetime.now(tz=timezone.utc) + timedelta(**addons)


def utc_timestamp(**addons: int) -> int:
    """Get current timestamp with option to add additional timedelta."""
    return int(utc_datetime(**addons).timestamp())


def make_optional(cls: type) -> type:
    """Check if the type hint is Optional."""
    # An Optional type will be represented as Union[type, NoneType]
    if get_origin(cls) is Union and type(None) in get_args(cls):
        return cls
    return cast(type, Optional[cls])


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
# logging.getLogger('passlib').setLevel(logging.ERROR)

__all__ = [
    "Emailer",
    "SendGridEmailer",
    "utc_datetime",
    "utc_timestamp",
    "logger",
]
