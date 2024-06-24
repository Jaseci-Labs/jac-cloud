"""User APIs."""

from os import getenv
from typing import Optional, cast

from bson import ObjectId

from fastapi import APIRouter, Request, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import ORJSONResponse

from fastapi_sso.sso.base import OpenID, SSOBase
from fastapi_sso.sso.facebook import FacebookSSO
from fastapi_sso.sso.fitbit import FitbitSSO
from fastapi_sso.sso.github import GithubSSO
from fastapi_sso.sso.gitlab import GitlabSSO
from fastapi_sso.sso.google import GoogleSSO
from fastapi_sso.sso.kakao import KakaoSSO
from fastapi_sso.sso.line import LineSSO
from fastapi_sso.sso.linkedin import LinkedInSSO
from fastapi_sso.sso.microsoft import MicrosoftSSO
from fastapi_sso.sso.naver import NaverSSO
from fastapi_sso.sso.notion import NotionSSO
from fastapi_sso.sso.twitter import TwitterSSO
from fastapi_sso.sso.yandex import YandexSSO

from ..models import NULL_BYTES, User
from ..models.ephemerals import AttachSSO, DetachSSO
from ..plugins import JCONTEXT, JacContext, Root
from ..securities import authenticator, create_code, create_token
from ..utils import logger

router = APIRouter(prefix="/sso", tags=["sso"])

User = User.model()  # type: ignore[misc]

SUPPORTED_PLATFORMS: dict[str, type[SSOBase]] = {
    "FACEBOOK": FacebookSSO,
    "FITBIT": FitbitSSO,
    "GITHUB": GithubSSO,
    "GITLAB": GitlabSSO,
    "GOOGLE": GoogleSSO,
    "KAKAO": KakaoSSO,
    "LINE": LineSSO,
    "LINKEDIN": LinkedInSSO,
    "MICROSOFT": MicrosoftSSO,
    "NAVER": NaverSSO,
    "NOTION": NotionSSO,
    "TWITTER": TwitterSSO,
    "YANDEX": YandexSSO,
}

SSO: dict[str, SSOBase] = {}
SSO_HOST = getenv("SSO_HOST", "http://localhost:8000/sso")

for platform, cls in SUPPORTED_PLATFORMS.items():
    if (client_id := getenv(f"{platform}_CLIENT_ID")) and (
        client_secret := getenv(f"{platform}_CLIENT_SECRET")
    ):
        options: dict[str, object] = {
            "client_id": client_id,
            "client_secret": client_secret,
        }

        if base_endpoint_url := getenv(f"{platform}_BASE_ENDPOINT_URL"):
            options["base_endpoint_url"] = base_endpoint_url

        if tenant := getenv(f"{platform}_TENANT"):
            options["tenant"] = tenant

        if allow_insecure_http := getenv(f"{platform}_ALLOW_INSECURE_HTTP"):
            options["allow_insecure_http"] = allow_insecure_http == "true"

        SSO[platform.lower()] = cls(**options)


@router.get("/{platform}/{operation}")
async def sso_operation(
    platform: str, operation: str, redirect_uri: Optional[str] = None
) -> Response:
    """SSO Login API."""
    if sso := SSO.get(platform):
        with sso:
            return await sso.get_login_redirect(
                redirect_uri=redirect_uri
                or f"{SSO_HOST}/{platform}/{operation}/callback"
            )
    return ORJSONResponse({"message": "Feature not yet implemented!"}, 501)


@router.get("/{platform}/{operation}/callback")
async def sso_callback(request: Request, platform: str, operation: str) -> Response:
    """SSO Login API."""
    if sso := SSO.get(platform):
        with sso:
            if open_id := await sso.verify_and_process(request):
                match operation:
                    case "login":
                        return await login(platform, open_id)
                    case "register":
                        return await register(request, platform, open_id)
                    case "attach":
                        return await attach(platform, open_id)
                    case _:
                        pass

    return ORJSONResponse({"message": "Feature not yet implemented!"}, 501)


@router.post("/attach", dependencies=authenticator)
async def sso_attach(request: Request, attach_sso: AttachSSO) -> ORJSONResponse:  # type: ignore
    """Generate token from user."""
    if SSO.get(attach_sso.platform):
        if await User.Collection.find_one(
            {
                "$or": [
                    {f"sso.{platform}.id": attach_sso.id},
                    {f"sso.{platform}.email": attach_sso.email},
                ]
            }
        ):
            return ORJSONResponse({"message": "Already Attached!"}, 403)

        await User.Collection.update_one(
            {"_id": ObjectId(request.auth_user.id)},
            {
                "$set": {
                    f"sso.{attach_sso.platform}": {
                        "id": attach_sso.id,
                        "email": attach_sso.email,
                    }
                }
            },
        )

        return ORJSONResponse({"message": "Successfully Updated SSO!"}, 200)
    return ORJSONResponse({"message": "Feature not yet implemented!"}, 501)


@router.delete("/detach", dependencies=authenticator)
async def sso_detach(request: Request, detach_sso: DetachSSO) -> ORJSONResponse:  # type: ignore
    """Generate token from user."""
    if SSO.get(detach_sso.platform):
        await User.Collection.update_one(
            {"_id": ObjectId(request.auth_user.id)},
            {"$unset": {f"sso.{detach_sso.platform}": 1}},
        )
        return ORJSONResponse({"message": "Successfully Updated SSO!"}, 200)
    return ORJSONResponse({"message": "Feature not yet implemented!"}, 501)


async def get_token(user: User) -> ORJSONResponse:  # type: ignore
    """Generate token from user."""
    user_json = user.serialize()
    token = await create_token(user_json)

    return ORJSONResponse(content={"token": token, "user": user_json})


async def login(platform: str, open_id: OpenID) -> Response:
    """Login user method."""
    if user := cast(
        User,  # type: ignore
        await User.Collection.find_one(
            {
                "$or": [
                    {f"sso.{platform}.id": open_id.id},
                    {f"sso.{platform}.email": open_id.email},
                ]
            }
        ),
    ):
        user = cast(User, user)  # type: ignore
        if not user.is_activated:
            User.send_verification_code(
                await create_code(ObjectId(user.id)), user.email
            )
            raise HTTPException(
                status_code=400,
                detail="Account not yet verified! Resending verification code...",
            )

        return await get_token(user)
    return ORJSONResponse({"message": "Not Existing!"}, 403)


async def register(request: Request, platform: str, open_id: OpenID) -> Response:
    """Register user method."""
    if user := await User.Collection.find_one(
        {
            "$or": [
                {f"sso.{platform}.id": open_id.id},
                {f"sso.{platform}.email": open_id.email},
            ]
        }
    ):
        return await get_token(cast(User, user))  # type: ignore

    JCONTEXT.set(JacContext(request, save_on_exit=False))
    async with await Root.Collection.get_session() as session:
        async with session.start_transaction():
            try:
                root = await Root.register(session=session)
                ureq: dict[str, object] = User.register_type()(
                    email=f"{root.id}-sso@jac-lang.org",
                    password=NULL_BYTES,
                    **User.sso_mapper(open_id),
                ).obfuscate()
                ureq["root_id"] = root.id
                ureq["is_activated"] = True
                ureq["sso"] = {platform: {"id": open_id.id, "email": open_id.email}}

                result = await User.Collection.insert_one(ureq, session=session)
                await session.commit_transaction()
            except Exception:
                logger.exception("Error commiting user registration!")
                result = None

                await session.abort_transaction()
    if result:
        return await login(platform, open_id)
    else:
        return ORJSONResponse({"message": "Registration Failed!"}, 409)


async def attach(platform: str, open_id: OpenID) -> Response:
    """Return openid ."""
    if await User.Collection.find_one(
        {
            "$or": [
                {f"sso.{platform}.id": open_id.id},
                {f"sso.{platform}.email": open_id.email},
            ]
        }
    ):
        return ORJSONResponse({"message": "Already Attached!"}, 403)

    return ORJSONResponse(
        {"platform": platform, "id": open_id.id, "email": open_id.email}, 200
    )
