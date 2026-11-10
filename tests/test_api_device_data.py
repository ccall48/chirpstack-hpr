import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import grpc
from chirpstack_api import api as chirpstack_api
from redis.exceptions import ConnectionError as RedisConnectionError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import api as hpr_api
from schemas import GetDeviceSyncRequest


def rpc_error(code):
    return grpc.aio.AioRpcError(code, (), (), details="test failure")


class DeviceDataTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.eui = "0102030405060708"
        self.key = "11" * 16
        self.client = AsyncMock()
        self.client.Get.return_value = chirpstack_api.GetDeviceResponse(
            device=chirpstack_api.Device(dev_eui=self.eui, name="dev", join_eui=self.eui)
        )
        self.client.GetActivation.return_value = chirpstack_api.GetDeviceActivationResponse(
            device_activation=chirpstack_api.DeviceActivation(
                dev_eui=self.eui, dev_addr="01020304", nwk_s_enc_key=self.key,
                app_s_key="22" * 16,
            )
        )
        self.client.GetKeys.return_value = chirpstack_api.GetDeviceKeysResponse(
            device_keys=chirpstack_api.DeviceKeys(dev_eui=self.eui, nwk_key="33" * 16)
        )
        self.redis = AsyncMock()
        self.redis.get.return_value = None
        for target, value in (
            ("_get_channel", AsyncMock(return_value=None)),
            ("_redis", self.redis),
            ("AUTH_TOKEN", [("authorization", "Bearer test")]),
        ):
            context = patch.object(hpr_api, target, value)
            context.start()
            self.addCleanup(context.stop)
        context = patch.object(hpr_api.api, "DeviceServiceStub", return_value=self.client)
        context.start()
        self.addCleanup(context.stop)

    async def test_uncached_read_preserves_session_keys_and_caches_observation_time(self):
        self.redis.get.return_value = json.dumps({"data": {"nwkSEncKey": "stale"}, "observed_at": 1})
        with patch.object(hpr_api.time, "time", return_value=123.0):
            data, observed_at = await hpr_api.get_device_data(self.eui, use_cache=False)
        parsed = GetDeviceSyncRequest(**data)
        self.assertEqual((parsed.nwkSEncKey, parsed.devAddr), (self.key, 16909060))
        self.assertEqual(parsed.nwkKey, "33" * 16)
        self.assertEqual(observed_at, 123.0)
        self.redis.get.assert_not_awaited()
        envelope = json.loads(self.redis.set.await_args.args[1])
        self.assertEqual(envelope, {"data": data, "observed_at": 123.0})
        for rpc in (self.client.Get, self.client.GetActivation, self.client.GetKeys):
            rpc.assert_awaited_once()
            self.assertGreater(rpc.await_args.kwargs["timeout"], 0)
            self.assertEqual(rpc.await_args.args[0].dev_eui, self.eui)

    async def test_cache_hit_returns_original_observation_time_without_rpc(self):
        cached = {"devEui": self.eui, "nwkSEncKey": "cached"}
        self.redis.get.return_value = json.dumps({"data": cached, "observed_at": 7.0})
        self.assertEqual(await hpr_api.get_device_data(self.eui), (cached, 7.0))
        self.client.Get.assert_not_awaited()
        self.redis.set.assert_not_awaited()

    async def test_legacy_cache_entry_is_refreshed(self):
        self.redis.get.return_value = json.dumps({"nwkSEncKey": "stale"})
        data, _ = await hpr_api.get_device_data(self.eui)
        self.assertEqual(data["nwkSEncKey"], self.key)
        self.client.Get.assert_awaited_once()

    async def test_missing_activation_is_a_confirmed_inactive_device(self):
        self.client.GetActivation.side_effect = rpc_error(grpc.StatusCode.NOT_FOUND)
        data, _ = await hpr_api.get_device_data(self.eui, use_cache=False)
        self.assertFalse(GetDeviceSyncRequest(**data).nwkSEncKey)
        self.client.GetKeys.assert_not_awaited()
        self.redis.set.assert_awaited_once()

    async def test_missing_root_keys_preserves_abp_activation(self):
        self.client.GetKeys.side_effect = rpc_error(grpc.StatusCode.NOT_FOUND)
        data, _ = await hpr_api.get_device_data(self.eui, use_cache=False)
        self.assertEqual(GetDeviceSyncRequest(**data).nwkSEncKey, self.key)

    async def test_rpc_outages_propagate_without_caching_a_false_deactivation(self):
        for rpc_name in ("Get", "GetActivation", "GetKeys"):
            with self.subTest(rpc=rpc_name):
                error = rpc_error(grpc.StatusCode.UNAVAILABLE)
                rpc = getattr(self.client, rpc_name)
                rpc.side_effect = error
                with self.assertRaises(grpc.aio.AioRpcError) as caught:
                    await hpr_api.get_device_data(self.eui, use_cache=False)
                self.assertIs(caught.exception, error)
                self.redis.set.assert_not_awaited()
                rpc.side_effect = None

    async def test_redis_outage_still_returns_fresh_api_data(self):
        self.redis.get.side_effect = RedisConnectionError("unavailable")
        self.redis.set.side_effect = RedisConnectionError("unavailable")
        with patch("builtins.print"):
            data, _ = await hpr_api.get_device_data(self.eui)
        self.assertEqual(data["nwkSEncKey"], self.key)
