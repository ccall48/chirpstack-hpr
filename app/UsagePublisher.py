import os
import logging
import datetime


def info_log(msg):
    logging.info("USAGE_PUBLISHER: %s" % msg)


def publish_usage_event(dev_eui, tenant_id, application_id, dc_used):
    provider = os.getenv('PUBLISH_USAGE_EVENTS_PROVIDER', None)

    utc_now = datetime.datetime.utcnow()
    usage_event = {
        'datetime': utc_now.isoformat(),
        'dev_eui': dev_eui,
        'tenant_id': tenant_id,
        'application_id': application_id,
        'dc_used': dc_used
    }
    print(usage_event)

    if provider == 'AWS_SQS':
        from SqsUsagePublisher import publish_to_sqs
        info_log("Publishing usage event to SQS: %s" % usage_event)
        publish_to_sqs(usage_event)

    else:
        info_log("Provider %s not found" % provider)
