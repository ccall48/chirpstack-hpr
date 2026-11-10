import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import api as hpr_api  # noqa: E402


class DiscoveryFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_tenant_apps_raises_on_partial_failure(self):
        with patch.object(hpr_api, "get_tenant_list", AsyncMock(return_value=["t1", "t2"])), \
             patch.object(hpr_api, "get_tennant_apps", AsyncMock(side_effect=[["app1"], RuntimeError("boom")])):
            with self.assertRaises(RuntimeError):
                await hpr_api.all_tenant_apps()

    async def test_all_tenant_apps_returns_combined_list_when_all_succeed(self):
        with patch.object(hpr_api, "get_tenant_list", AsyncMock(return_value=["t1", "t2"])), \
             patch.object(hpr_api, "get_tennant_apps", AsyncMock(side_effect=[["app1"], ["app2", "app3"]])):
            result = await hpr_api.all_tenant_apps()

        self.assertEqual(result, ["app1", "app2", "app3"])

    async def test_all_tenant_deveui_raises_on_partial_failure(self):
        with patch.object(hpr_api, "all_tenant_apps", AsyncMock(return_value=["app1", "app2"])), \
             patch.object(hpr_api, "get_application_devices", AsyncMock(side_effect=[["dev1"], RuntimeError("boom")])):
            with self.assertRaises(RuntimeError):
                await hpr_api.all_tenant_deveui()
