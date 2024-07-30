"""Apple SSO."""

from typing import Any, Dict, Literal, Optional, Union

from fastapi import Request

from fastapi_sso.sso.base import OpenID
from fastapi_sso.sso.google import GoogleSSO as _GoogleSSO

from google.auth.jwt import decode

from httpx import get


class GoogleSSO(_GoogleSSO):
    """Class providing login via Google Android OAuth."""

    id_token_issuer = "https://accounts.google.com"
    oauth_cert_url = "https://www.googleapis.com/oauth2/v1/certs"

    async def verify_and_process(  # type: ignore[override]
        self,
        request: Request,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        redirect_uri: Optional[str] = None,
        convert_response: Union[Literal[True], Literal[False]] = True,
    ) -> Union[Optional[OpenID], Optional[Dict[str, Any]]]:
        """Verify and process Apple SSO."""
        if id_token := request.query_params.get("id_token"):
            return await self.get_open_id(id_token)
        return await super().verify_and_process(
            request,
            params=params,
            headers=headers,
            redirect_uri=redirect_uri,
            convert_response=convert_response,
        )

    async def get_open_id(self, id_token: str) -> OpenID:
        """Get OpenID from id_tokens provided by Apple."""
        identity_data: dict = decode(
            id_token, certs=get(self.oauth_cert_url).json(), audience=self.client_id
        )

        return OpenID(id=identity_data.get("sub"), email=identity_data.get("email"))
