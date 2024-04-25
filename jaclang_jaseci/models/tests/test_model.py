"""Temporary."""

from os import getenv
from unittest.async_case import IsolatedAsyncioTestCase

from bson import ObjectId

from passlib.hash import pbkdf2_sha512

from .. import User
from ...collections import BaseCollection


class JacLangModelTests(IsolatedAsyncioTestCase):
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
        # register user
        user = User.register_type()(email="testing@gmail.com", password="12345678")
        user = user.obfuscate()
        user["root_id"] = ObjectId()
        user_id = await User.Collection.insert_one(user)

        self.assertIsInstance(user_id, ObjectId)

        user = await User.Collection.find_by_email("testing@gmail.com")
        self.assertIsInstance(user, User)
        self.assertEqual("testing@gmail.com", user.email)
        self.assertTrue(pbkdf2_sha512.verify("12345678", user.password))

        # OVerriden User Scenario
        class OverridenUser(User):
            field1: str
            field2: int

        # register user
        user = User.register_type()(
            email="testing2@gmail.com",
            password="anypassword",
            field1="testing",
            field2=1,
        )
        user = user.obfuscate()
        user["root_id"] = ObjectId()
        user_id = await User.Collection.insert_one(user)

        self.assertIsInstance(user_id, ObjectId)

        user = await User.Collection.find_by_email("testing2@gmail.com")
        self.assertIsInstance(user, User)
        self.assertEqual("testing2@gmail.com", user.email)
        self.assertTrue(pbkdf2_sha512.verify("anypassword", user.password))
        self.assertTrue("testing", user.field1)
        self.assertTrue(2, user.field2)
