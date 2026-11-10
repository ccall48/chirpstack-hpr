import asyncio
import grpc
from unittest.mock import AsyncMock, patch

from tests.helpers import DatabaseTestCase, device
import app
import HeliumProtos
from models import device_row
from schemas import GetDeviceSyncRequest


class DeviceSyncTests(DatabaseTestCase):
    async def run_passes(self, responses, passes=1, discovery_error=None):
        state = app.SyncState()
        state.complete = True  # A failed refresh must revoke a previous successful pass.
        discover = AsyncMock(return_value=list(responses), side_effect=discovery_error)

        async def get(dev_eui, use_cache):
            self.assertFalse(use_cache)
            response = responses[dev_eui]
            if isinstance(response, Exception):
                raise response
            return response, 1.0

        with patch.object(app, "all_tenant_deveui", discover), patch.object(
            app, "get_device_data", get
        ), patch.object(app, "route_id", self.hpr.route_id), patch(
            "app.asyncio.sleep", side_effect=[None] * (passes - 1) + [asyncio.CancelledError()]
        ), patch("builtins.print"):
            with self.assertRaises(asyncio.CancelledError):
                await app.devices_sync_upsert(self.hpr, state)
        self.assertFalse(self.hpr.sync_lock.locked())
        return state

    async def test_success_commits_before_registering_and_enables_purge(self):
        data = device()

        async def send(updates):
            self.assertEqual(await self.stored_key(), data["nwkSEncKey"])
            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0].session_key, data["nwkSEncKey"])
            self.assertEqual(updates[0].devaddr, int(data["devAddr"], 16))
            self.assertEqual(updates[0].action, 0)

        self.hpr.route_skfs.side_effect = send
        state = await self.run_passes({data["devEui"]: data})
        self.assertTrue(state.complete)
        self.hpr.route_skfs.assert_awaited_once()

    async def test_invalid_or_unavailable_device_blocks_purge_but_syncs_good_devices(self):
        good = device()
        for bad in ({"missing": "fields"}, RuntimeError("ChirpStack unavailable")):
            with self.subTest(bad=type(bad).__name__):
                self.hpr.route_skfs.reset_mock()
                state = await self.run_passes({good["devEui"]: good, "bad": bad})
                self.assertFalse(state.complete)
                self.assertEqual(await self.stored_key(), good["nwkSEncKey"])
                self.hpr.route_skfs.assert_awaited_once()
                with patch.object(self.db, "get_stale_skfs", AsyncMock()) as select:
                    self.assertEqual(await self.hpr.remove_stale_skfs(state), 0)
                    select.assert_not_awaited()

    async def test_discovery_failure_revokes_previous_permission_to_purge(self):
        state = await self.run_passes({}, discovery_error=RuntimeError("partial inventory"))
        self.assertFalse(state.complete)
        self.hpr.route_skfs.assert_not_awaited()

    async def test_database_failure_prevents_registration_and_purge(self):
        data = device()
        with patch.object(self.db, "upsert_device", AsyncMock(side_effect=RuntimeError("disk full"))):
            state = await self.run_passes({data["devEui"]: data})
        self.assertFalse(state.complete)
        self.hpr.route_skfs.assert_not_awaited()

    async def test_helium_failure_keeps_purge_disabled(self):
        data = device()
        self.hpr.route_skfs.side_effect = RuntimeError("Helium unavailable")
        state = await self.run_passes({data["devEui"]: data})
        self.assertFalse(state.complete)
        self.assertEqual(await self.stored_key(), data["nwkSEncKey"])

    async def test_helium_reconciliation_recovers_after_more_than_five_failed_passes(self):
        data = device()
        self.hpr.route_skfs.side_effect = [RuntimeError("unavailable")] * 6 + [None]
        state = await self.run_passes({data["devEui"]: data}, passes=7)
        self.assertTrue(state.complete)
        self.assertEqual(self.hpr.route_skfs.await_count, 7)

    async def test_join_waits_for_refresh_read_and_remote_write_then_stores_fresh_key(self):
        old, new = device(key="11" * 16), device(key="22" * 16)
        read_started, allow_read = asyncio.Event(), asyncio.Event()
        send_started, allow_send = asyncio.Event(), asyncio.Event()
        join_started = asyncio.Event()
        fresh_read = AsyncMock(return_value=(new, 2.0))

        async def get(*args, **kwargs):
            read_started.set()
            await allow_read.wait()
            return old, 1.0

        async def send(updates):
            if updates[0].session_key == old["nwkSEncKey"]:
                send_started.set()
                await allow_send.wait()

        async def join():
            join_started.set()
            await self.hpr.sync_device(new["devEui"])

        self.hpr.route_skfs.side_effect = send
        with patch.object(app, "all_tenant_deveui", AsyncMock(return_value=[old["devEui"]])), \
             patch.object(app, "get_device_data", get), \
             patch.object(HeliumProtos, "get_device_data", fresh_read), \
             patch.object(app, "route_id", self.hpr.route_id), \
             patch("app.asyncio.sleep", side_effect=asyncio.CancelledError), \
             patch("builtins.print"):
            refresh = self.start_task(app.devices_sync_upsert(self.hpr, app.SyncState()))
            await asyncio.wait_for(read_started.wait(), 2)
            joined = self.start_task(join())
            await asyncio.wait_for(join_started.wait(), 2)
            fresh_read.assert_not_awaited()
            allow_read.set()
            await asyncio.wait_for(send_started.wait(), 2)
            fresh_read.assert_not_awaited()
            allow_send.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(refresh, 2)
            await asyncio.wait_for(joined, 2)
        fresh_read.assert_awaited_once_with(new["devEui"], use_cache=False)
        self.assertEqual(await self.stored_key(), new["nwkSEncKey"])
        self.assertEqual([c.args[0][0].session_key for c in self.hpr.route_skfs.await_args_list],
                         [old["nwkSEncKey"], new["nwkSEncKey"]])

    async def test_cancelling_a_refresh_releases_lock_and_keeps_purge_disabled(self):
        entered = asyncio.Event()

        async def get(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

        state = app.SyncState()
        state.complete = True
        with patch.object(app, "all_tenant_deveui", AsyncMock(return_value=[device()["devEui"]])), \
             patch.object(app, "get_device_data", get):
            refresh = self.start_task(app.devices_sync_upsert(self.hpr, state))
            await asyncio.wait_for(entered.wait(), 2)
            refresh.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await refresh
        self.assertFalse(state.complete)
        self.assertFalse(self.hpr.sync_lock.locked())
        self.assertIsNone(await self.stored_key())
        self.hpr.route_skfs.assert_not_awaited()

    async def test_device_policy_controls_eui_and_skf_actions(self):
        for policy, action in (({}, 0), ({"isDisabled": True}, 1),
                               ({"tags": {"private": True}}, 1)):
            with self.subTest(policy=policy):
                data = device() | policy
                data["variables"] = {"max_copies": 3}
                self.hpr.route_euis.reset_mock()
                self.hpr.route_skfs.reset_mock()
                with patch.object(HeliumProtos, "get_device_data", AsyncMock(return_value=(data, 1.0))):
                    await self.hpr.sync_device(data["devEui"], update_euis=True)
                self.hpr.route_euis.assert_awaited_once_with(
                    int(data["devEui"], 16), int(data["joinEui"], 16), action
                )
                self.hpr.route_skfs.assert_awaited_once()
                update = self.hpr.route_skfs.await_args.args[0][0]
                self.assertEqual((update.action, update.session_key, update.max_copies),
                                 (action, data["nwkSEncKey"], 3))

    async def test_deleted_device_event_is_skipped_but_rpc_outage_propagates(self):
        for code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.UNAVAILABLE):
            with self.subTest(code=code):
                error = grpc.aio.AioRpcError(code, (), (), details="test failure")
                with patch.object(HeliumProtos, "get_device_data", AsyncMock(side_effect=error)), \
                     patch("builtins.print"):
                    if code == grpc.StatusCode.NOT_FOUND:
                        await self.hpr.sync_device(device()["devEui"])
                    else:
                        with self.assertRaises(grpc.aio.AioRpcError):
                            await self.hpr.sync_device(device()["devEui"])
                self.assertIsNone(await self.stored_key())
                self.hpr.route_skfs.assert_not_awaited()
                self.assertFalse(self.hpr.sync_lock.locked())


class StaleSkfTests(DatabaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.state = app.SyncState()
        self.state.complete = True

    async def test_failed_removal_preserves_candidates_until_successful_retry(self):
        await self.seed_skfs(["stale"])
        self.hpr.route_skfs.side_effect = RuntimeError("timeout")
        with self.assertRaisesRegex(RuntimeError, "timeout"):
            await self.hpr.remove_stale_skfs(self.state)
        self.assertEqual(await self.stored_skfs(), {"stale"})
        self.assertFalse(self.hpr.sync_lock.locked())
        self.hpr.route_skfs.side_effect = None
        self.assertEqual(await self.hpr.remove_stale_skfs(self.state), 1)
        self.assertEqual(await self.stored_skfs(), set())
        update = self.hpr.route_skfs.await_args.args[0][0]
        self.assertEqual((update.session_key, update.devaddr, update.action),
                         ("stale", 16909060, 1))

    async def test_rechecks_key_ownership_after_waiting_for_lock(self):
        await self.check_waiting_purge("claim-key")

    async def test_rechecks_sync_completion_after_waiting_for_lock(self):
        await self.check_waiting_purge("incomplete-refresh")

    async def check_waiting_purge(self, change):
        await self.seed_skfs(["11" * 16])
        selected = asyncio.Event()
        real_select = self.db.get_stale_skfs

        async def select(route_id):
            candidates = await real_select(route_id)
            self.assertEqual(len(candidates), 1)
            selected.set()
            return candidates

        await self.hpr.sync_lock.acquire()
        try:
            with patch.object(self.db, "get_stale_skfs", select):
                purge = self.start_task(self.hpr.remove_stale_skfs(self.state))
                await asyncio.wait_for(selected.wait(), 2)
                if change == "claim-key":
                    await self.db.upsert_device([
                        device_row(GetDeviceSyncRequest(**device()), 1.0, "test-route")
                    ])
                else:
                    self.state.complete = False
        finally:
            self.hpr.sync_lock.release()
        self.assertEqual(await asyncio.wait_for(purge, 2), 0)
        self.hpr.route_skfs.assert_not_awaited()
        self.assertEqual(await self.stored_skfs(), {"11" * 16})

    async def test_join_cannot_read_or_register_during_a_removal_rpc(self):
        data = device()
        await self.seed_skfs([data["nwkSEncKey"]])
        sending, allow_send, join_started = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def send(updates):
            if updates[0].action == 1:
                sending.set()
                await allow_send.wait()

        async def join():
            join_started.set()
            await self.hpr.sync_device(data["devEui"])

        self.hpr.route_skfs.side_effect = send
        with patch.object(HeliumProtos, "get_device_data", AsyncMock(return_value=(data, 1.0))) as get:
            purge = self.start_task(self.hpr.remove_stale_skfs(self.state))
            await asyncio.wait_for(sending.wait(), 2)
            joined = self.start_task(join())
            await asyncio.wait_for(join_started.wait(), 2)
            get.assert_not_awaited()
            self.assertEqual(await self.stored_skfs(), {data["nwkSEncKey"]})
            allow_send.set()
            self.assertEqual(await asyncio.wait_for(purge, 2), 1)
            await asyncio.wait_for(joined, 2)
        self.assertEqual(await self.stored_key(), data["nwkSEncKey"])
        self.assertEqual([c.args[0][0].action for c in self.hpr.route_skfs.await_args_list], [1, 0])

    async def test_chunk_failure_retains_unsent_and_failed_chunks(self):
        keys = [f"{i:032x}" for i in range(251)]
        await self.seed_skfs(keys)
        self.hpr.route_skfs.side_effect = [None, RuntimeError("timeout")]
        with self.assertRaises(RuntimeError):
            await self.hpr.remove_stale_skfs(self.state)
        calls = self.hpr.route_skfs.await_args_list
        successful = {u.session_key for u in calls[0].args[0]}
        self.assertEqual(len(successful), 100)
        self.assertEqual(await self.stored_skfs(), set(keys) - successful)
        self.hpr.route_skfs.reset_mock(side_effect=True)
        self.assertEqual(await self.hpr.remove_stale_skfs(self.state), 151)
        self.assertEqual([len(c.args[0]) for c in self.hpr.route_skfs.await_args_list], [100, 51])
        self.assertEqual(await self.stored_skfs(), set())
