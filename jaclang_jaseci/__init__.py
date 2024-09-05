"""JacLang FastAPI."""

from dotenv import find_dotenv, load_dotenv

import nest_asyncio

from .core import FastAPI

nest_asyncio.apply()

load_dotenv(find_dotenv(), override=True)

start = FastAPI.start

__all__ = ["FastAPI", "start"]
