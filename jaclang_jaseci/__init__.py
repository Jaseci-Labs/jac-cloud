"""JacLang Jaseci."""

from dotenv import find_dotenv, load_dotenv

import nest_asyncio

from .jaseci import FastAPI

nest_asyncio.apply()

load_dotenv(find_dotenv(), override=True)

__all__ = ["FastAPI"]
