"""User APIs."""

from typing import cast

from fastapi import APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import ORJSONResponse

from pymongo.client_session import ClientSession

from ..models import User
from ..models.ephemerals import UserRequest
from ..plugins import Root
from ..securities import create_token, verify
from ..utils import logger

router = APIRouter(prefix="/user", tags=["user"])

User = User.model()  # type: ignore[misc]


@router.post("/register", status_code=status.HTTP_200_OK)
async def register(req: User.register_type()) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    async with await Root.Collection.get_session() as session:  # type: ignore[attr-defined]
        async with cast(ClientSession, session).start_transaction():  # type: ignore[attr-defined]
            try:

                root = await Root.register(session=session)
                req_obf = req.obfuscate()
                req_obf["root_id"] = root.id
                result = await User.Collection.insert_one(req_obf, session=session)

                await cast(ClientSession, session).commit_transaction()  # type: ignore[func-returns-value]
            except Exception:
                logger.exception("Error commiting user registration!")
                result = None

                await cast(ClientSession, session).abort_transaction()  # type: ignore[func-returns-value]

    if result:
        return ORJSONResponse({"message": "Successfully Registered!"}, 201)
    else:
        return ORJSONResponse({"message": "Registration Failed!"}, 409)


@router.post("/login")
async def root(req: UserRequest) -> ORJSONResponse:
    """Login user API."""
    user: User = await User.Collection.find_by_email(req.email)  # type: ignore
    if not user or not verify(req.password, user.password):
        raise HTTPException(status_code=400, detail="Invalid Email/Password!")
    user_json = user.serialize()
    token = await create_token(user_json)

    return ORJSONResponse(content={"token": token, "user": user_json})
