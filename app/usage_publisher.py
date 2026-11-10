import hashlib
import os
from datetime import datetime, timezone

from helium_func import data_bytes_size


def uplink_dc_used(uplink):
    hotspots = sum(
        gw.get("metadata", {}).get("network") == "helium_iot"
        for gw in uplink.get("rxInfo", [])
    )
    return data_bytes_size(uplink.get("data", "")) * hotspots


def create_usage_publisher():
    if os.getenv("PUBLISH_USAGE_EVENTS", "false").lower() != "true":
        return None
    if not os.getenv("ROUTE_ID", "").strip():
        raise ValueError("ROUTE_ID is required when publishing usage")
    provider = os.getenv("PUBLISH_USAGE_EVENTS_PROVIDER")
    if provider == "AWS_SQS":
        from publishers.sqs_usage_publisher import SqsUsagePublisher

        queue_url = os.getenv("PUBLISH_USAGE_EVENTS_SQS_URL", "").strip()
        if not queue_url:
            raise ValueError(
                "PUBLISH_USAGE_EVENTS_SQS_URL is required when publishing usage"
            )
        return SqsUsagePublisher(
            queue_url, os.getenv("PUBLISH_USAGE_EVENTS_SQS_REGION", "us-east-1")
        )
    raise ValueError(f"Unsupported usage publisher: {provider}")


async def publish_usage_event(publisher, uplink, message_id, route_id):
    dc_used = uplink_dc_used(uplink)
    if dc_used == 0:
        return
    if isinstance(message_id, bytes):
        message_id = message_id.decode()
    device = uplink["deviceInfo"]
    # Use event time (UTC) so retries have the same body.
    if uplink.get("time"):
        timestamp = datetime.fromisoformat(uplink["time"].replace("Z", "+00:00"))
    else:
        timestamp = datetime.fromtimestamp(
            int(message_id.split("-")[0]) / 1000, timezone.utc
        )
    event = {
        "datetime": timestamp.astimezone(timezone.utc).replace(tzinfo=None).isoformat(),
        "dev_eui": str(device["devEui"]),
        "tenant_id": str(device["tenantId"]),
        "application_id": str(device["applicationId"]),
        "dc_used": dc_used,
    }
    identity = uplink.get("deduplicationId") or message_id
    event_id = hashlib.sha256(f"{route_id}:{identity}".encode()).hexdigest()
    # Providers accept the same event and stable ID; failed sends must raise for retry.
    await publisher.publish(event, event_id)
