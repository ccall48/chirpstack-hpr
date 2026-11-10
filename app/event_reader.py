# These streams are ChirpStack's own live-tail feeds for its web UI (API request
# log, device event log), not a durable event bus. ChirpStack trims each to
# [monitoring] api_request_log_max_history / device_event_log_max_history entries
# (default 10 — see chirpstack/src/config.rs), so checkpointing here only protects
# against a restart shorter than that retention window. Raise those values in the
# ChirpStack config to whatever your worst-case outage/restart time requires;
# without that, this module still resumes correctly but events can still be lost.

# TODO: Move the above explanation to section in README.md when it exists.


class EventReader:
    def __init__(self, redis, streams, checkpoint_key=None, tail_streams=()):
        self.redis = redis
        self.offsets = {name: "0-0" for name in streams}
        self.checkpoint_key = checkpoint_key
        self.tail_streams = tail_streams

    async def initialize(self):
        if self.checkpoint_key is None:
            return
        for name in self.offsets:
            offset = await self.redis.hget(self.checkpoint_key, name)
            if offset is None:
                # Snapshot accounting streams before startup sync to avoid re-accounting history.
                # Other streams still replay retained requests on first start.
                tail = (
                    await self.redis.xrevrange(name, count=1)
                    if name in self.tail_streams
                    else []
                )
                offset = tail[0][0] if tail else "0-0"
                await self.redis.hset(self.checkpoint_key, name, offset)
            self.offsets[name] = offset

    async def read(self):
        response = await self.redis.xread(streams=self.offsets, count=1, block=1000)
        return [
            (name.decode() if isinstance(name, bytes) else name, message)
            for name, messages in response
            for message in messages
        ]

    async def acknowledge(self, stream_name, message_id):
        # Commit only after all handlers (including the usage send) have succeeded.
        if self.checkpoint_key is not None:
            await self.redis.hset(self.checkpoint_key, stream_name, message_id)
        self.offsets[stream_name] = message_id
