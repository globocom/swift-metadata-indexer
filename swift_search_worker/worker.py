#!/usr/bin/env python

import config
import json
import logger
import sys

from utils import ElasticSearchUtils, queue_connection, queue_channel

log = logger.logger(__name__.split('.')[-1])

elastic_utils = ElasticSearchUtils(config.TOKEN_ENDPOINT,
                                   config.CLIENT_ID,
                                   config.CLIENT_SECRET,
                                   config.ES_URL)


def callback(ch, method, properties, body):
    global elastic_utils

    try:
        msg, created = elastic_utils.send_to_elastic(json.loads(body))
        if created:
            # If the message was sent to ES, ack
            # Otherwise, keep it on queue
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            log.error('Failed to create message on Elastic Search')
    except ValueError:
        log.error('Invalid message')


if __name__ == '__main__':

    connection = queue_connection(username=config.QUEUE_USERNAME,
                                  password=config.QUEUE_PASSWORD,
                                  host=config.QUEUE_URL,
                                  vhost=config.QUEUE_VHOST) or sys.exit(1)

    channel = queue_channel(connection, config.QUEUE_NAME) or sys.exit(1)

    channel.basic_consume(callback, config.QUEUE_NAME)

    try:
        log.info('Starting consumer')
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info('Stoping consumer')
        channel.stop_consuming()

    log.debug('Closing queue connection')
    connection.close()
