"""Temporary."""

from contextlib import suppress
from json import load
from os import getenv
from unittest.async_case import IsolatedAsyncioTestCase

from httpx import get, post

from ..collections import BaseCollection


class JacLangJaseciTests(IsolatedAsyncioTestCase):
    """JacLang Jaseci Feature Tests."""

    async def asyncSetUp(self) -> None:
        """Reset DB and wait for server."""
        self.host = "http://0.0.0.0:8000"
        self.client = BaseCollection.get_client()
        self.database = getenv("DATABASE_NAME", "jaclang")
        await self.client.drop_database(self.database)
        count = 0
        while True:
            if count > 5:
                self.get_openapi_json(1)
                break
            else:
                with suppress(Exception):
                    self.get_openapi_json(1)
                    break
            count += 1

    async def asyncTearDown(self) -> None:
        """Clean up DB."""
        await self.client.drop_database(self.database)

    def create_test_user(self) -> None:
        """Call openapi specs json."""
        res = post(
            f"{self.host}/user/register",
            json={"password": "string", "email": "user@example.com", "name": "string"},
        )
        res.raise_for_status()
        self.assertEqual({"message": "Successfully Registered!"}, res.json())

        res = post(
            f"{self.host}/user/login",
            json={"email": "user@example.com", "password": "string"},
        )
        res.raise_for_status()
        res = res.json()

        token = res.get("token")
        self.assertIsNotNone(token)
        self.headers = {"Authorization": f"Bearer {token}"}

        user = res.get("user")
        self.assertEqual("user@example.com", user["email"])
        self.assertEqual("string", user["name"])
        self.user = user

    def get_openapi_json(self, timeout: int = None) -> dict:
        """Call openapi specs json."""
        res = get(f"{self.host}/openapi.json", timeout=timeout)
        res.raise_for_status()
        return res.json()

    def post_api(self, api: str, json: dict = None) -> dict:
        """Call post api return json."""
        res = post(f"{self.host}/walker/{api}", json=json, headers=self.headers)
        res.raise_for_status()
        return res.json()

    def test_all_features(self) -> None:
        """Test all features."""
        ############################################################
        # ------------------ TEST OPENAPI SPECS ------------------ #
        ############################################################

        res = self.get_openapi_json()

        with open("jaclang_jaseci/tests/simple_walkerapi_openapi_specs.json") as file:
            self.assertEqual(load(file), res)

        ############################################################
        # --------------------- CREATE USER ---------------------- #
        ############################################################

        self.create_test_user()

        ############################################################
        # ---------------------- TEST GRAPH ---------------------- #
        ############################################################

        res = self.post_api("create_sample_graph")
        self.assertEqual({"status": 200, "returns": [True]}, res)

        res = self.post_api("visit_sample_graph")
        self.assertEqual(200, res["status"])
        self.assertEqual([1, 2, 3], res["returns"])

        reports = res["reports"]
        report = reports[0]
        self.assertTrue(report["id"].startswith("n::"))

        report = reports[1]
        self.assertTrue(report["id"].startswith("n:boy:"))
        self.assertEqual({"val1": "a", "val2": "b"}, report["context"])

        report = reports[2]
        self.assertTrue(report["id"].startswith("n:girl:"))
        self.assertEqual({"val": "b"}, report["context"])

        res = self.post_api("update_sample_graph")
        self.assertEqual(200, res["status"])
        self.assertEqual([None, None], res["returns"])

        reports = res["reports"]
        report = reports[0]
        self.assertTrue(report["id"].startswith("n:girl:"))
        self.assertEqual({"val": "b"}, report["context"])

        report = reports[1]
        self.assertTrue(report["id"].startswith("n:girl:"))
        self.assertEqual({"val": "new"}, report["context"])

        res = self.post_api("update_inner_field_sample_graph")
        self.assertEqual(200, res["status"])
        self.assertEqual([None], res["returns"])

        report = res["reports"][0]
        self.assertTrue(report["id"].startswith("n:someone"))
        self.assertEqual({"val": []}, report["context"])

        res = self.post_api(f"update_inner_field_sample_graph/{report['id']}")
        self.assertEqual(200, res["status"])
        self.assertEqual([None], res["returns"])

        report = res["reports"][0]
        self.assertTrue(report["id"].startswith("n:someone"))
        self.assertEqual({"val": [1]}, report["context"])

        res = self.post_api(f"update_inner_field_sample_graph/{report['id']}")
        self.assertEqual(200, res["status"])
        self.assertEqual([None], res["returns"])

        report = res["reports"][0]
        self.assertTrue(report["id"].startswith("n:someone"))
        self.assertEqual({"val": [1, 1]}, report["context"])
