"""User APIs."""

from os import getenv

from fastapi import APIRouter, Request, status
from fastapi.exceptions import HTTPException
from fastapi.responses import ORJSONResponse

from passlib.hash import pbkdf2_sha512

from pymongo.errors import ConnectionFailure, OperationFailure

from ..dtos import (
    UserChangePassword,
    UserForgotPassword,
    UserRequest,
    UserResetPassword,
    UserVerification,
)
from ..models import User as BaseUser
from ..security import (
    authenticator,
    create_code,
    create_token,
    invalidate_token,
    verify_code,
)
from ..utils import Emailer, logger
from ...core.architype import BulkWrite, NodeAnchor, Root


RESTRICT_UNVERIFIED_USER = getenv("RESTRICT_UNVERIFIED_USER") == "true"
router = APIRouter(prefix="/user", tags=["user"])

User = BaseUser.model()  # type: ignore[misc]


@router.post("/register", status_code=status.HTTP_200_OK)
async def register(req: User.register_type()) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    async with await User.Collection.get_session() as session:
        async with session.start_transaction():
            root = Root().__jac__

            req_obf: dict = req.obfuscate()
            req_obf["root_id"] = root.id
            is_activated = req_obf["is_activated"] = not Emailer.has_client()

            retry = 0
            max_retry = BulkWrite.SESSION_MAX_TRANSACTION_RETRY
            while retry <= max_retry:
                try:
                    await NodeAnchor.Collection.insert_one(root.serialize(), session)
                    if id := (
                        await User.Collection.insert_one(req_obf, session=session)
                    ).inserted_id:
                        await BulkWrite.commit(session)
                        if not is_activated:
                            User.send_verification_code(
                                await create_code(id), req.email
                            )
                        return ORJSONResponse(
                            {"message": "Successfully Registered!"}, 201
                        )
                except (ConnectionFailure, OperationFailure) as ex:
                    if ex.has_error_label("TransientTransactionError"):
                        retry += 1
                        logger.error(
                            "Error executing bulk write! "
                            f"Retrying [{retry}/{max_retry}] ..."
                        )
                        continue
                    logger.exception("Error executing bulk write!")
                    await session.abort_transaction()
                    break
                except Exception:
                    logger.exception("Error executing bulk write!")
                    await session.abort_transaction()
                    break
    return ORJSONResponse({"message": "Registration Failed!"}, 409)


@router.post(
    "/send-verification-code",
    status_code=status.HTTP_200_OK,
    dependencies=authenticator,
)
async def send_verification_code(request: Request) -> ORJSONResponse:
    """Verify user API."""
    user: BaseUser = request._user
    if user.is_activated:
        return ORJSONResponse({"message": "Account is already verified!"}, 400)
    else:
        User.send_verification_code(await create_code(user.id), user.email)
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
    user: BaseUser = await User.Collection.find_by_email(req.email)  # type: ignore
    if not user or not pbkdf2_sha512.verify(req.password, user.password):
        raise HTTPException(status_code=400, detail="Invalid Email/Password!")

    if RESTRICT_UNVERIFIED_USER and not user.is_activated:
        User.send_verification_code(await create_code(user.id), req.email)
        raise HTTPException(
            status_code=400,
            detail="Account not yet verified! Resending verification code...",
        )

    user_json = user.serialize()
    token = await create_token(user_json)

    return ORJSONResponse(content={"token": token, "user": user_json})


@router.post(
    "/change_password", status_code=status.HTTP_200_OK, dependencies=authenticator
)
async def change_password(request: Request, ucp: UserChangePassword) -> ORJSONResponse:  # type: ignore
    """Register user API."""
    user: BaseUser | None = getattr(request, "_user", None)
    if user:
        with_pass = await User.Collection.find_by_email(user.email)
        if (
            with_pass
            and pbkdf2_sha512.verify(ucp.old_password, with_pass.password)
            and await User.Collection.update_one(
                {"_id": user.id},
                {"$set": {"password": pbkdf2_sha512.hash(ucp.new_password).encode()}},
            )
        ):
            await invalidate_token(user.id)
            return ORJSONResponse({"message": "Successfully Updated!"}, 200)
    return ORJSONResponse({"message": "Update Failed!"}, 403)


@router.post("/forgot_password", status_code=status.HTTP_200_OK)
async def forgot_password(ufp: UserForgotPassword) -> ORJSONResponse:
    """Forgot password API."""
    user = await User.Collection.find_by_email(ufp.email)
    if isinstance(user, User):
        User.send_reset_code(await create_code(user.id, True), user.email)
        return ORJSONResponse({"message": "Reset password email sent!"}, 200)
    else:
        return ORJSONResponse({"message": "Failed to process forgot password!"}, 403)


@router.post("/reset_password", status_code=status.HTTP_200_OK)
async def reset_password(urp: UserResetPassword) -> ORJSONResponse:
    """Reset password API."""
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