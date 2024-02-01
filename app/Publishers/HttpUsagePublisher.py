import os
import urllib3
from . import tenant_usage_pb2


usage_endpoint = os.getenv('HTTP_PUBLISHER_ENDPOINT', None)

timeout = urllib3.Timeout(connect=2.0, read=7.0)
HTTP = urllib3.PoolManager(timeout=timeout)


def publish_to_http(usage_event: dict):
    event = tenant_usage_pb2.TenantUsage()
    event.datetime = usage_event['datetime']
    event.dev_eui = usage_event['dev_eui']
    event.tenant_id = usage_event['tenant_id']
    event.application_id = usage_event['application_id']
    event.dc_used = usage_event['dc_used']
    payload = event.SerializeToString()

    r = HTTP.request(
        'POST',
        usage_endpoint,
        body=payload,
        headers={'Content-Type': 'application/octet-stream'}
    )
    return r
