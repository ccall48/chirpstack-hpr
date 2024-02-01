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

    # Here maybe we could add more providers, e.g. GCP PubSub / Kafka or something.

    match provider:
        case 'AWS_SQS':
            from Publishers.SqsUsagePublisher import publish_to_sqs
            info_log("Publishing usage event to SQS: %s" % usage_event)
            publish_to_sqs(usage_event)

        case 'POSTGRES':
            from Publishers.PgUsagePublisher import publish_to_pg
            info_log("Publishing usage event to PG: %s" % usage_event)
            publish_to_pg(usage_event)

        case 'HTTP':
            from Publishers.HttpUsagePublisher import publish_to_http
            info_log("Publishing usage event to HTTP: %s" % usage_event)
            publish_to_http(usage_event)

        case _:
            info_log("Provider %s not found" % provider)
            return
