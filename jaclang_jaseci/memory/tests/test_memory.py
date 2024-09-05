"""Temporary."""

from unittest.async_case import IsolatedAsyncioTestCase

from .. import BaseMemory


class JacLangMemoryTests(IsolatedAsyncioTestCase):
    """JacLang Jaseci Memory Tests."""

    async def asyncSetUp(self) -> None:
        """Initialize DB connection and cleanup."""
        BaseMemory.__redis__ = None
        self.rd_client = BaseMemory.get_rd()
        await self.rd_client.flushall()

    async def asyncTearDown(self) -> None:
        """Clean up DB."""
        await self.rd_client.flushall()

    async def test_crud_operations(self) -> None:
        """Test crud operations."""

        class SampleMemory(BaseMemory):
            __table__: str = "sample"

        # get all keys: should be 0 record
        keys = await SampleMemory.keys()
        self.assertEqual(0, len(keys))

        # insert one
        self.assertTrue(await SampleMemory.set("key-1", "val-1"))

        # get all keys: should be 1 record
        keys = await SampleMemory.keys()
        self.assertEqual([b"key-1"], keys)

        # get one by key
        self.assertEqual("val-1", await SampleMemory.get("key-1"))

        # get one by key
        self.assertTrue(await SampleMemory.delete("key-1"))

        # get one by key: already deleted
        self.assertIsNone(await SampleMemory.get("key-1"))

        # get all hkeys: should be 0 record
        hkeys = await SampleMemory.hkeys()
        self.assertEqual(0, len(hkeys))

        # insert one on hash
        self.assertTrue(await SampleMemory.hset("key-1", "val-1"))

        # get all keys: should be 1 record
        hkeys = await SampleMemory.hkeys()
        self.assertEqual([b"key-1"], hkeys)

        # get one by key
        self.assertEqual("val-1", await SampleMemory.hget("key-1"))

        # get one by key
        self.assertTrue(await SampleMemory.hdelete("key-1"))

        # get one by key: already deleted
        self.assertIsNone(await SampleMemory.hget("key-1"))
