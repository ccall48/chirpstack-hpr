import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from chirpstack_api import api as chirpstack_api

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import api as hpr_api  # noqa: E402


def devices_page(dev_euis, total_count):
    return chirpstack_api.ListDevicesResponse(
        total_count=total_count,
        result=[chirpstack_api.DeviceListItem(dev_eui=eui) for eui in dev_euis],
    )


class PaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_collects_every_page_past_the_first(self):
        # 2500 devices in one application: 3 pages at PAGE_SIZE=1000.
        euis = [f"{i:016x}" for i in range(2500)]
        client = AsyncMock()
        client.List.side_effect = [
            devices_page(euis[0:1000], 2500),
            devices_page(euis[1000:2000], 2500),
            devices_page(euis[2000:2500], 2500),
        ]
        with patch.object(hpr_api.api, "DeviceServiceStub", return_value=client), patch.object(
            hpr_api, "_get_channel", AsyncMock(return_value=None)
        ):
            result = await hpr_api.get_application_devices("app-id")

        self.assertEqual(result, euis)
        self.assertEqual(client.List.call_count, 3)
        offsets = [call.args[0].offset for call in client.List.call_args_list]
        self.assertEqual(offsets, [0, 1000, 2000])

    async def test_incomplete_listing_raises_instead_of_returning_partial_inventory(self):
        client = AsyncMock()
        client.List.side_effect = [devices_page([], 2500)]
        with patch.object(hpr_api.api, "DeviceServiceStub", return_value=client), patch.object(
            hpr_api, "_get_channel", AsyncMock(return_value=None)
        ):
            with self.assertRaisesRegex(RuntimeError, "incomplete listing"):
                await hpr_api.get_application_devices("app-id")

        self.assertEqual(client.List.call_count, 1)

    async def test_single_page_under_the_limit(self):
        client = AsyncMock()
        client.List.side_effect = [devices_page(["01" * 8], 1)]
        with patch.object(hpr_api.api, "DeviceServiceStub", return_value=client), patch.object(
            hpr_api, "_get_channel", AsyncMock(return_value=None)
        ):
            result = await hpr_api.get_application_devices("app-id")

        self.assertEqual(result, ["01" * 8])
        client.List.assert_awaited_once()
