"""User APIs."""

import json
from os import getenv

from bson import ObjectId

from fastapi import APIRouter, Request, status
from fastapi.exceptions import HTTPException
from fastapi.responses import ORJSONResponse

import orjson

from passlib.hash import pbkdf2_sha512

from ..models import User
from ..models.ephemerals import (
    UserChangePassword,
    UserForgotPassword,
    UserRequest,
    UserResetPassword,
    UserVerification,
)
from ..plugins import JCONTEXT, JacContext, Root
from ..securities import (
    authenticator,
    create_code,
    create_token,
    invalidate_token,
    verify_code,
)
from ..utils import Emailer, logger


RESTRICT_UNVERIFIED_USER = getenv("RESTRICT_UNVERIFIED_USER") == "true"
router = APIRouter(prefix="/user", tags=["user"])

User = User.model()  # type: ignore[misc]


@router.post("/register", status_code=status.HTTP_200_OK)
async def register(request: Request, req: User.register_type()) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    log_dict = {
        "api_name": "register",
        "caller_name": req.email,
        "payload": json.dumps(req.model_dump_json()),
    }
    log_dict["extra_fields"] = list(log_dict.keys())
    logger.info(f"Incoming call: {log_dict}", extra=log_dict)

    JCONTEXT.set(JacContext(request, save_on_exit=False))
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
        resp = ORJSONResponse({"message": "Successfully Registered!"}, 201)
    else:
        resp = ORJSONResponse({"message": "Registration Failed!"}, 409)

    log_dict["response"] = json.dumps(orjson.loads(resp.body))
    log_dict["extra_fields"] = list(log_dict.keys())
    logger.info(
        f"Returning call: {log_dict}",
        extra=log_dict,
    )
    return resp


@router.post(
    "/send-verification-code",
    status_code=status.HTTP_200_OK,
    dependencies=authenticator,
)
async def send_verification_code(request: Request) -> ORJSONResponse:
    """Verify user API."""
    user = request.auth_user
    if user.is_activated:
        return ORJSONResponse({"message": "Account is already verified!"}, 400)
    else:
        User.send_verification_code(await create_code(ObjectId(user.id)), user.email)
        return ORJSONResponse({"message": "Successfully sent verification code!"}, 200)


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
    log_dict = {
        "api_name": "login",
        "caller_name": req.email,
        "payload": json.dumps(req.model_dump_json()),
    }
    log_dict["extra_fields"] = list(log_dict.keys())
    logger.info(f"Incoming call: {log_dict}", extra=log_dict)

    user: User = await User.Collection.find_by_email(req.email)  # type: ignore
    if not user or not pbkdf2_sha512.verify(req.password, user.password):
        raise HTTPException(status_code=400, detail="Invalid Email/Password!")

    if RESTRICT_UNVERIFIED_USER and not user.is_activated:
        User.send_verification_code(await create_code(ObjectId(user.id)), req.email)
        raise HTTPException(
            status_code=400,
            detail="Account not yet verified! Resending verification code...",
        )

    user_json = user.serialize()
    token = await create_token(user_json)

    resp = {"token": token, "user": user_json}
    log_dict["response"] = json.dumps(resp)
    log_dict["extra_fields"] = list(log_dict.keys())
    logger.info(
        f"Returning call: {log_dict}",
        extra=log_dict,
    )

    return ORJSONResponse(content=resp)


@router.post(
    "/change_password", status_code=status.HTTP_200_OK, dependencies=authenticator
)
async def change_password(request: Request, ucp: UserChangePassword) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    user: User | None = getattr(request, "auth_user", None)  # type: ignore
    if user:
        with_pass = await User.Collection.find_by_email(user.email)
        if (
            isinstance(with_pass, User)
            and pbkdf2_sha512.verify(ucp.old_password, with_pass.password)
            and await User.Collection.update_one(
                {"_id": ObjectId(user.id)},
                {"$set": {"password": pbkdf2_sha512.hash(ucp.new_password).encode()}},
            )
        ):
            await invalidate_token(user.id)
            return ORJSONResponse({"message": "Successfully Updated!"}, 200)
    return ORJSONResponse({"message": "Update Failed!"}, 403)


@router.post("/forgot_password", status_code=status.HTTP_200_OK)
async def forgot_password(ufp: UserForgotPassword) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    user = await User.Collection.find_by_email(ufp.email)
    if isinstance(user, User):
        User.send_reset_code(await create_code(ObjectId(user.id), True), user.email)
        return ORJSONResponse({"message": "Reset password email sent!"}, 200)
    else:
        return ORJSONResponse({"message": "Failed to process forgot password!"}, 403)


@router.post("/reset_password", status_code=status.HTTP_200_OK)
async def reset_password(request: Request, urp: UserResetPassword) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    if (
        user_id := await verify_code(urp.code, True)
    ) and await User.Collection.update_by_id(
        user_id,
        {
            "$set": {
                "password": pbkdf2_sha512.hash(urp.password).encode(),
                "is_activated": True,
            }
        },
    ):
        await invalidate_token(user_id)
        return ORJSONResponse({"message": "Password reset successfully!"}, 200)

    return ORJSONResponse({"message": "Failed to reset password!"}, 403)
