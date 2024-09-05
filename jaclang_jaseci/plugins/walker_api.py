"""Walker API Plugin."""

import json
from asyncio import get_event_loop
from dataclasses import Field, _MISSING_TYPE, dataclass, is_dataclass
from inspect import iscoroutine
from os import getenv
from pydoc import locate
from re import compile
from typing import Any, Callable, Optional, Type, TypeVar, Union, cast

from fastapi import APIRouter, Depends, File, Request, Response, UploadFile
from fastapi.responses import ORJSONResponse

from jaclang.core.construct import (
    Architype,
    DSFunc,
    WalkerAnchor as _WalkerAnchor,
    WalkerArchitype as _WalkerArchitype,
    root,
)
from jaclang.plugin.default import hookimpl
from jaclang.plugin.feature import JacFeature as Jac

from orjson import loads

from pydantic import BaseModel, Field as pyField, ValidationError, create_model

from starlette.datastructures import UploadFile as BaseUploadFile

from .common import JCONTEXT, JacContext
from ..securities import authenticator
from ..utils import logger, make_optional


T = TypeVar("T")
DISABLE_AUTO_ENDPOINT = getenv("DISABLE_AUTO_ENDPOINT") == "true"
PATH_VARIABLE_REGEX = compile(r"{([^\}]+)}")
FILE = {
    "File": UploadFile,
    "Files": list[UploadFile],
    "OptFile": Optional[UploadFile],
    "OptFiles": Optional[list[UploadFile]],
}

router = APIRouter(prefix="/walker", tags=["walker"])


class DefaultSpecs:
    """Default API specs."""

    path: str = ""
    methods: list[str] = ["post"]
    as_query: Union[str, list[str]] = []
    auth: bool = True
    private: bool = False


@dataclass(eq=False)
class WalkerAnchor(_WalkerAnchor):
    """Overriden WalkerAnchor."""

    async def await_if_coroutine(self, ret: Any) -> Any:  # noqa: ANN401
        """Await return if it's a coroutine."""
        if iscoroutine(ret):
            ret = await ret
        return ret

    async def spawn_call(self, nd: Architype) -> "WalkerArchitype":
        """Invoke data spatial call."""
        self.path: list = []
        self.next = [nd]
        self.returns = []

        while len(self.next):
            nd = self.next.pop(0)
            for i in nd._jac_entry_funcs_:
                if not i.trigger or isinstance(self.obj, i.trigger):
                    if i.func:
                        self.returns.append(
                            await self.await_if_coroutine(i.func(nd, self.obj))
                        )
                    else:
                        raise ValueError(f"No function {i.name} to call.")
                if self.disengaged:
                    return self.obj
            for i in self.obj._jac_entry_funcs_:
                if not i.trigger or isinstance(nd, i.trigger):
                    if i.func:
                        self.returns.append(
                            await self.await_if_coroutine(i.func(self.obj, nd))
                        )
                    else:
                        raise ValueError(f"No function {i.name} to call.")
                if self.disengaged:
                    return self.obj
            for i in self.obj._jac_exit_funcs_:
                if not i.trigger or isinstance(nd, i.trigger):
                    if i.func:
                        self.returns.append(
                            await self.await_if_coroutine(i.func(self.obj, nd))
                        )
                    else:
                        raise ValueError(f"No function {i.name} to call.")
                if self.disengaged:
                    return self.obj
            for i in nd._jac_exit_funcs_:
                if not i.trigger or isinstance(self.obj, i.trigger):
                    if i.func:
                        self.returns.append(
                            await self.await_if_coroutine(i.func(nd, self.obj))
                        )
                    else:
                        raise ValueError(f"No function {i.name} to call.")
                if self.disengaged:
                    return self.obj
        self.ignores: list = []
        return self.obj


class WalkerArchitype(_WalkerArchitype):
    """Walker Architype Protocol."""

    def __init__(self) -> None:
        """Create walker architype."""
        self._jac_: WalkerAnchor = WalkerAnchor(obj=self)


class JacPlugin:
    """Plugin Methods."""

    @staticmethod
    @hookimpl
    def make_walker(
        on_entry: list[DSFunc], on_exit: list[DSFunc]
    ) -> Callable[[type], type]:
        """Create a walker architype."""

        def decorator(cls: Type[Architype]) -> Type[Architype]:
            """Decorate class."""
            cls = Jac.make_architype(
                cls=cls, arch_base=WalkerArchitype, on_entry=on_entry, on_exit=on_exit
            )
            populate_apis(cls)
            return cls

        return decorator

    @staticmethod
    @hookimpl
    def spawn_call(op1: Architype, op2: Architype) -> WalkerArchitype:
        """Jac's spawn operator feature."""
        return get_event_loop().run_until_complete(JacPlugin._spawn_call(op1, op2))

    @staticmethod
    @hookimpl
    async def _spawn_call(op1: Architype, op2: Architype) -> WalkerArchitype:
        """Jac's spawn operator feature."""
        if isinstance(op1, WalkerArchitype):
            return await op1._jac_.spawn_call(op2)
        elif isinstance(op2, WalkerArchitype):
            return await op2._jac_.spawn_call(op1)
        else:
            raise TypeError("Invalid walker object")

    @staticmethod
    @hookimpl
    def get_root() -> Architype:
        """Jac's assign comprehension feature."""
        jctx: JacContext = JacContext.get_context()
        current_root = root
        if jctx:
            current_root = jctx.root
        return current_root

    @staticmethod
    @hookimpl
    def report(expr: Any) -> Any:  # noqa: ANN401
        """Jac's report stmt feature."""
        jctx: JacContext = JacContext.get_context()
        jctx.report(expr)


def get_specs(cls: type) -> Optional[Type[DefaultSpecs]]:
    """Get Specs and inherit from DefaultSpecs."""
    specs = getattr(cls, "Specs", None)
    if specs is None:
        if DISABLE_AUTO_ENDPOINT:
            return None
        specs = DefaultSpecs

    if not issubclass(specs, DefaultSpecs):
        specs = type(specs.__name__, (specs, DefaultSpecs), {})

    return specs


def gen_model_field(cls: type, field: Field, is_file: bool = False) -> tuple[type, Any]:
    """Generate Specs for Model Field."""
    if not isinstance(field.default, _MISSING_TYPE):
        consts = (make_optional(cls), pyField(default=field.default))
    elif callable(field.default_factory):
        consts = (make_optional(cls), pyField(default_factory=field.default_factory))
    else:
        consts = (cls, File(...) if is_file else ...)

    return consts


def populate_apis(cls: type) -> None:
    """Generate FastAPI endpoint based on WalkerArchitype class."""
    if (specs := get_specs(cls)) and not specs.private:
        path: str = specs.path or ""
        methods: list = specs.methods or []
        as_query: Union[str, list] = specs.as_query or []
        auth: bool = specs.auth or False

        query: dict[str, Any] = {}
        body: dict[str, Any] = {}
        files: dict[str, Any] = {}

        if path:
            if not path.startswith("/"):
                path = f"/{path}"
            if isinstance(as_query, list):
                as_query += PATH_VARIABLE_REGEX.findall(path)

        if is_dataclass(cls):
            fields: dict[str, Field] = cls.__dataclass_fields__
            for key, val in fields.items():
                if file_type := FILE.get(cast(str, val.type)):  # type: ignore[arg-type]
                    files[key] = gen_model_field(file_type, val, True)  # type: ignore[arg-type]
                else:
                    consts = gen_model_field(locate(val.type), val)  # type: ignore[arg-type]

                    if as_query == "*" or key in as_query:
                        query[key] = consts
                    else:
                        body[key] = consts

        payload: dict[str, Any] = {
            "query": (
                create_model(f"{cls.__name__.lower()}_query_model", **query),
                Depends(),
            ),
            "files": (
                create_model(f"{cls.__name__.lower()}_files_model", **files),
                Depends(),
            ),
        }

        body_model = None
        if body:
            body_model = create_model(f"{cls.__name__.lower()}_body_model", **body)

            if files:
                payload["body"] = (UploadFile, File(...))
            else:
                payload["body"] = (body_model, ...)

        payload_model = create_model(f"{cls.__name__.lower()}_request_model", **payload)

        async def api_entry(
            request: Request,
            node: Optional[str],
            payload: payload_model = Depends(),  # type: ignore # noqa: B008
        ) -> ORJSONResponse:
            pl = cast(BaseModel, payload).model_dump()
            body = pl.get("body", {})

            if isinstance(body, BaseUploadFile) and body_model:
                body = loads(await body.read())
                try:
                    body = body_model(**body).model_dump()
                except ValidationError as e:
                    return ORJSONResponse({"detail": e.errors()})

            jctx = JacContext(request=request, entry=node)
            JCONTEXT.set(jctx)

            caller = getattr(request, "auth_user", None)
            try:
                payload_json = json.dumps(await request.json())
            except Exception:
                payload_json = ""

            log_dict = {
                "api_name": cls.__name__,
                "caller_name": caller.email if caller else "",
                "payload": payload_json,
                "entry_node": node,
            }
            log_msg = str(
                f"Incoming call from {log_dict['caller_name']}"
                f" to {log_dict['api_name']}"
                f" with payload: {log_dict['payload']}"
                f" at entry node: {log_dict['entry_node']}"
            )
            log_dict["extra_fields"] = list(log_dict.keys())
            logger.info(log_msg, extra=log_dict)
            wlk: WalkerAnchor = cls(**body, **pl["query"], **pl["files"])._jac_
            await wlk.spawn_call(await jctx.get_entry())
            await jctx.clean_up()
            resp = jctx.response(wlk.returns)
            log_dict["api_response"] = json.dumps(resp)
            log_dict["extra_fields"] = list(log_dict.keys())
            log_msg = str(
                f"Returning call from {log_dict['caller_name']}"
                f" to {log_dict['api_name']}"
                f" with payload: {log_dict['payload']}"
                f" at entry node: {log_dict['entry_node']}"
                f" with response: {log_dict['api_response']}"
            )
            logger.info(
                log_msg,
                extra=log_dict,
            )
            return ORJSONResponse(resp)

        async def api_root(
            request: Request,
            payload: payload_model = Depends(),  # type: ignore # noqa: B008
        ) -> Response:
            return await api_entry(request, None, payload)

        for method in methods:
            method = method.lower()

            walker_method = getattr(router, method)

            settings: dict[str, list[Any]] = {"tags": ["walker"]}
            if auth:
                settings["dependencies"] = cast(list, authenticator)

            walker_method(url := f"/{cls.__name__}{path}", summary=url, **settings)(
                api_root
            )
            walker_method(
                url := f"/{cls.__name__}/{{node}}{path}", summary=url, **settings
            )(api_entry)


def specs(
    cls: Optional[Type[T]] = None,
    *,
    path: str = "",
    methods: list[str] = ["post"],  # noqa: B006
    as_query: Union[str, list] = [],  # noqa: B006
    auth: bool = True,
) -> Callable:
    """Walker Decorator."""

    def wrapper(cls: Type[T]) -> Type[T]:
        if get_specs(cls) is None:
            p = path
            m = methods
            aq = as_query
            a = auth

            class Specs(DefaultSpecs):
                path: str = p
                methods: list[str] = m
                as_query: Union[str, list] = aq
                auth: bool = a

            cls.Specs = Specs  # type: ignore[attr-defined]

            populate_apis(cls)
        return cls

    if cls:
        return wrapper(cls)

    return wrapper
