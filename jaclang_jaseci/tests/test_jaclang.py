"""Temporary."""

import sys
from io import StringIO
from unittest import TestCase

from jaclang.cli.cli import run


class JacLangTests(TestCase):
    """JacLang Jaseci Feature Tests."""

    def setUp(self) -> None:
        """Override stdout."""
        self.stdout = sys.stdout
        sys.stdout = StringIO()

    def tearDown(self) -> None:
        """Restoring stdout."""
        sys.stdout = self.stdout

    def test_simple_code(self) -> None:
        """Test simple jaclang."""
        run("jaclang_jaseci/tests/simple_walker.jac")
        self.assertEqual("nested1\nnested2\nnested3\nsample\n", sys.stdout.getvalue())
