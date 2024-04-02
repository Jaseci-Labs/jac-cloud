"""User APIs."""

from bson import ObjectId

from fastapi import APIRouter, status
from fastapi.exceptions import HTTPException
from fastapi.responses import ORJSONResponse

from passlib.hash import pbkdf2_sha512

from ..models import User
from ..models.ephemerals import UserRequest, UserVerification
from ..plugins import Root
from ..securities import create_code, create_token, verify_code
from ..utils import Emailer, logger

router = APIRouter(prefix="/user", tags=["user"])

User = User.model()  # type: ignore[misc]


@router.post("/register", status_code=status.HTTP_200_OK)
async def register(req: User.register_type()) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    async with await Root.Collection.get_session() as session:
        async with session.start_transaction():
            try:

                root = await Root.register(session=session)
                req_obf: dict = req.obfuscate()
                req_obf["root_id"] = root.id
                is_activated = req_obf["is_activated"] = not Emailer.has_client()

                result = await User.Collection.insert_one(req_obf, session=session)
                if result and not is_activated:
                    User.send_verification_code(await create_code(result), req.email)
                await session.commit_transaction()
            except Exception:
                logger.exception("Error commiting user registration!")
                result = None

                await session.abort_transaction()

    if result:
        return ORJSONResponse({"message": "Successfully Registered!"}, 201)
    else:
        return ORJSONResponse({"message": "Registration Failed!"}, 409)


@router.post("/verify")
async def verify(req: UserVerification) -> ORJSONResponse:
    """Verify user API."""
    if (user_id := await verify_code(req.code)) and await User.Collection.update_by_id(
        user_id, {"$set": {"is_activated": True}}
    ):
        return ORJSONResponse({"message": "Successfully Verified!"}, 200)

    return ORJSONResponse({"message": "Verification Failed!"}, 403)


@router.post("/login")
async def root(req: UserRequest) -> ORJSONResponse:
    """Login user API."""
    user: User = await User.Collection.find_by_email(req.email)  # type: ignore
    if not user or not pbkdf2_sha512.verify(req.password, user.password):
        raise HTTPException(status_code=400, detail="Invalid Email/Password!")

    if not user.is_activated:
        User.send_verification_code(await create_code(ObjectId(user.id)), req.email)
        raise HTTPException(
            status_code=400,
            detail="Account not yet verified! Resending verification code...",
        )

    user_json = user.serialize()
    token = await create_token(user_json)

    return ORJSONResponse(content={"token": token, "user": user_json})
