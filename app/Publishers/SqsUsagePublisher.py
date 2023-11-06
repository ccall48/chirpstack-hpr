import boto3
import os
import json
import logging

from botocore.exceptions import ClientError


def info_log(msg):
    logging.info("SQS_USAGE_PUBLISHER: %s" % msg)


def publish_to_sqs(event):
    queue_url = os.getenv('PUBLISH_USAGE_EVENTS_SQS_URL')
    aws_region = os.getenv("PUBLISH_USAGE_EVENTS_SQS_REGION", "us-east-1")
    info_log("Publishing usage event to SQS (%s): %s" % (queue_url, event))

    sqs_client = boto3.client("sqs", region_name=aws_region)
    dedup_id = '%s-%s-%s' % (
        event.get('datetime'),
        event.get('dev_eui'),
        event.get('application_id')
    )

    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(event),
            MessageDeduplicationId=dedup_id,
            MessageGroupId='HeliumUsageEvents'
        )
    except ClientError:
        info_log(f'Could not send meessage to the - {queue_url}.')
    else:
        return response
