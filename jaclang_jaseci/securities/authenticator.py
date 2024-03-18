"""Walker API Authenticator."""

from os import getenv
from random import choice
from string import ascii_letters, digits
from typing import Any, Optional, cast

from bson import ObjectId

from fastapi import Depends, Request
from fastapi.exceptions import HTTPException
from fastapi.security import HTTPBearer

from jwt import decode, encode

from passlib.context import CryptContext

from ..memory import CodeMemory, TokenMemory
from ..models import User
from ..plugins import Root
from ..utils import logger, utc_timestamp


TOKEN_SECRET = getenv(
    "TOKEN_SECRET", "".join(choice(ascii_letters + digits) for _ in range(50))
)
TOKEN_ALGORITHM = getenv("TOKEN_ALGORITHM", "HS256")


def encrypt(data: dict) -> str:
    """Encrypt data."""
    return encode(data, key=TOKEN_SECRET, algorithm=TOKEN_ALGORITHM)


def decrypt(token: str) -> Optional[dict]:
    """Decrypt data."""
    try:
        return decode(token, key=TOKEN_SECRET, algorithms=[TOKEN_ALGORITHM])
    except Exception:
        logger.exception("Token is invalid!")
        return None


async def create_code(user_id: ObjectId) -> str:
    """Generate Verification Code."""
    verification = encrypt(
        {
            "user_id": str(user_id),
            "expiration": utc_timestamp(
                hours=int(getenv("VERIFICATION_TIMEOUT") or "24")
            ),
        }
    )
    if await CodeMemory.hset(key=verification, data=True):
        return verification
    raise HTTPException(500, "Verification Creation Failed!")


async def verify_code(code: str) -> Optional[str]:
    """Verify Code."""
    decrypted = decrypt(code)
    if (
        decrypted
        and decrypted["expiration"] > utc_timestamp()
        and await CodeMemory.hget(key=code)
    ):
        return decrypted["user_id"]
    return None


async def create_token(user: dict[str, Any]) -> str:
    """Generate token for current user."""
    user["expiration"] = utc_timestamp(hours=int(getenv("TOKEN_TIMEOUT") or "12"))
    token = encrypt(user)
    if await TokenMemory.hset(key=token, data=True):
        return token
    raise HTTPException(500, "Token Creation Failed!")


async def authenticate(request: Request) -> None:
    """Authenticate current request and attach authenticated user and their root."""
    authorization = request.headers.get("Authorization")
    if authorization and authorization.lower().startswith("bearer"):
        token = authorization[7:]
        decrypted = decrypt(token)
        if (
            decrypted
            and decrypted["expiration"] > utc_timestamp()
            and await TokenMemory.hget(key=token)
        ):
            user = cast(User, await User.model().Collection.find_by_id(decrypted["id"]))
            root = cast(Root, await Root.Collection.find_by_id(user.root_id))
            root._jac_doc_.current_access_level = 1
            request.auth_user = user  # type: ignore[attr-defined]
            request.auth_root = root  # type: ignore[attr-defined]
            return

    raise HTTPException(status_code=401)


verify_pass = CryptContext(schemes=["bcrypt"], deprecated="auto").verify
authenticator = [Depends(HTTPBearer()), Depends(authenticate)]
