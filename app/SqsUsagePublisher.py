import boto3
import logging


def info_log(msg):
    logging.info("SQS_USAGE_PUBLISHER: %s" % msg)


def publish_to_sqs(event):
    info_log("Publishing usage event to SQS: %s" % event)
