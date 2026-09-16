from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

import config
from api_server import _get_pool


class ApiServerStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_pool_creation_does_not_run_legacy_migrations(self) -> None:
        expected_pool = object()
        with (
            patch.object(config, "DATABASE_URL", "postgresql://test"),
            patch("api_server.asyncpg.create_pool", new=AsyncMock(return_value=expected_pool)) as create_pool,
        ):
            result = await _get_pool()

        self.assertIs(result, expected_pool)
        create_pool.assert_awaited_once_with(dsn="postgresql://test", min_size=1, max_size=5)

    async def test_pool_creation_requires_database_url(self) -> None:
        with patch.object(config, "DATABASE_URL", ""):
            with self.assertRaisesRegex(RuntimeError, "DATABASE_URL missing"):
                await _get_pool()
