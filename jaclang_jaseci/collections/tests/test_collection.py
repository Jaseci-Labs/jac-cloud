"""Temporary."""

from os import getenv
from unittest.async_case import IsolatedAsyncioTestCase

from bson import ObjectId

from pymongo import InsertOne, UpdateOne

from .. import BaseCollection


class JacLangCollectionTests(IsolatedAsyncioTestCase):
    """JacLang Jaseci Collection Tests."""

    async def asyncSetUp(self) -> None:
        """Initialize DB connection and cleanup."""
        BaseCollection.__client__ = None
        BaseCollection.__database__ = None
        self.db_client = BaseCollection.get_client()
        self.db_name = getenv("DATABASE_NAME", "jaclang")
        await self.db_client.drop_database(self.db_name)

    async def asyncTearDown(self) -> None:
        """Clean up DB."""
        await self.db_client.drop_database(self.db_name)

    async def test_crud_operations(self) -> None:
        """Test crud operations."""

        class SampleCollection(BaseCollection[dict]):
            __collection__: str = "sample"
            __excluded__: list[str] = ["hidden_field"]

        # get all records: should be 0 record
        agen = await SampleCollection.find()
        self.assertEqual(0, sum([1 async for _ in agen]))

        # insert one record
        id = await SampleCollection.insert_one({"val": "testing-1"})
        self.assertIsInstance(id, ObjectId)

        # get all records: should be 1 record
        records = 0
        async for data in await SampleCollection.find():
            records += 1
            self.assertEqual(id, data.get("_id"))
            self.assertEqual("testing-1", data.get("val"))
        self.assertEqual(1, records)

        # get one record by id
        data = await SampleCollection.find_by_id(id)
        self.assertEqual(id, data.get("_id"))
        self.assertEqual("testing-1", data.get("val"))

        ids = await SampleCollection.insert_many(
            [
                {"val": "testing-2"},
                {"val": "testing-3"},
                {"val": "testing-4"},
                {"val": "testing-5"},
            ]
        )
        ids.insert(0, id)

        # get all records: should be 5 records
        records = 0
        async for data in await SampleCollection.find():
            records += 1
            self.assertEqual(ids.pop(0), data.get("_id"))
            self.assertEqual(f"testing-{records}", data.get("val"))
        self.assertEqual(5, records)

        # delete one: delete count == 0
        self.assertEqual(1, await SampleCollection.delete_by_id(id))

        # redeleting same by id: delete count == 0
        self.assertEqual(0, await SampleCollection.delete_by_id(id))

        # get all records: should be 4 records
        agen = await SampleCollection.find()
        self.assertEqual(4, sum([1 async for _ in agen]))

        # get one record
        data = await SampleCollection.find_one()
        self.assertIsNotNone(data)

        # update by id
        self.assertEqual(
            1,
            await SampleCollection.update_by_id(
                data["_id"], {"$set": {"val": "testing-updated"}}
            ),
        )

        # check if updated
        updated = await SampleCollection.find_by_id(data["_id"])
        self.assertEqual("testing-updated", updated["val"])

        # update many by filter: empty filter
        self.assertEqual(
            3,
            await SampleCollection.update_many(
                {}, {"$set": {"val": "testing-updated"}}
            ),
        )

        # everything should be val=="testing-updated"
        async for data in await SampleCollection.find():
            self.assertEqual("testing-updated", data.get("val"))

        # update one by filter: empty filter
        self.assertEqual(
            1,
            await SampleCollection.update_one(
                {}, {"$set": {"val": "testing-updated-2"}}
            ),
        )

        # get all with val="testing-updated-2": should be 1 record only
        agen = await SampleCollection.find({"val": "testing-updated-2"})
        self.assertEqual(1, sum([1 async for _ in agen]))

        # delete one no filter
        self.assertEqual(1, await SampleCollection.delete_one({}))

        # get all records: should be 3 records
        agen = await SampleCollection.find()
        self.assertEqual(3, sum([1 async for _ in agen]))

        # delete everything left
        self.assertEqual(3, await SampleCollection.delete({}))

        # get all records: should 0 record
        agen = await SampleCollection.find()
        self.assertEqual(0, sum([1 async for _ in agen]))

        id = await SampleCollection.insert_one({"val": "testing-1"})
        self.assertIsInstance(id, ObjectId)

        # bulk write
        result = await SampleCollection.bulk_write(
            [
                InsertOne({"val": "testing-2"}),
                UpdateOne({"_id": id}, {"$set": {"val": "updated"}}),
            ]
        )

        self.assertEqual(1, result["nModified"])
        self.assertEqual(1, result["nInserted"])

        # transactional
        async with await SampleCollection.get_session() as session:
            async with session.start_transaction():
                try:
                    # bulk write
                    result = await SampleCollection.bulk_write(
                        [
                            InsertOne({"val": "testing-3"}),
                            InsertOne({"val": "testing-4"}),
                            UpdateOne({"_id": id}, {"$set": {"val": "testing-1"}}),
                        ],
                        session=session,
                    )
                    await session.commit_transaction()
                except Exception:
                    await session.abort_transaction()

        self.assertEqual(1, result["nModified"])
        self.assertEqual(2, result["nInserted"])

        # get all records: should be 3 records
        records = 0
        async for data in await SampleCollection.find():
            records += 1
            self.assertEqual(f"testing-{records}", data.get("val"))
        self.assertEqual(4, records)

        # transactional with abort
        async with await SampleCollection.get_session() as session:
            async with session.start_transaction():
                try:
                    # bulk write
                    result = await SampleCollection.bulk_write(
                        [
                            InsertOne({"val": "testing-5"}),
                            UpdateOne(
                                {"_id": id}, {"$set": {"val": "testing-updated"}}
                            ),
                        ],
                        session=session,
                    )
                    print({"test": 1}["throw_error"])
                    await session.commit_transaction()
                except Exception:
                    await session.abort_transaction()

        # get all records: should be 3 records
        records = 0
        async for data in await SampleCollection.find():
            records += 1
            self.assertEqual(f"testing-{records}", data.get("val"))
        self.assertEqual(4, records)
