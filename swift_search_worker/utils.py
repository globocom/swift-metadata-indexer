import json
import logger
import pika
import urllib

from alf.client import Client

log = logger.logger(__name__.split('.')[-1])


def queue_connection(username, password, host, vhost, port=5672):
    credentials = pika.PlainCredentials(username, password)

    params = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials
    )

    try:
        connection = pika.BlockingConnection(params)
        log.debug('Queue connection OK')
    except (pika.exceptions.ConnectionClosed, Exception):
        log.error('Fail to connect to RabbitMQ')
        return None

    return connection


def queue_channel(connection, queue_name):

    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        log.debug('Queue Channel OK')
    except (pika.exceptions.ConnectionClosed, Exception):
        log.exception('Fail to create channel')
        return None

    return channel


def get_obj_info(data):
    """
    Creating the document info that will be sent to ES
    Also extracting the object info for it's id
    account/container/object

    :param data dict Object metadata receive from queue
    :returns dict with headers account, container, object and headers
    """
    try:
        uri = data.get('uri', '').split('/')
    except AttributeError:
        log.error('Fail to extract info from msg: uri is None <{}>')
        return None

    try:
        acc = uri[2].replace('AUTH_', '')
        con = uri[3]
        obj = '/'.join(uri[4:])
    except IndexError:
        log.error('Fail to extract info from msg: invalid uri: <{}>'.format(
            data.get('uri')))
        return None

    info = {}
    info['project_id'] = acc
    info['container'] = con
    info['object'] = obj
    if data.get('http_method') != 'DELETE':
        info['headers'] = data.get('headers', '')
        info['timestamp'] = data.get('timestamp', '')

    return info


class ElasticSearchUtils(object):

    def __init__(self, token_endpoint, client_id, client_secret, es_url):
        self.token_endpoint = token_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.es_url = es_url

        self.client = self.get_alf_client()

    def send_to_elastic(self, data):
        # ES document _id will be account_id/container/object (url_encoded)

        obj_info = get_obj_info(data)

        if not obj_info:
            return 'Invalid object info', False

        obj_url = self._get_es_obj_url(obj_info)

        if data['http_method'] == 'POST':
            log.info("POST to {} with data {}".format(obj_url, obj_info))
            try:
                res = self.client.post(obj_url, data=json.dumps(obj_info),
                                   headers={'Content-Type':
                                            'application/json'})
            except Exception as e:
                log.exception("Unable to POST data to ES")
                return "Unable to POST data to ES", False

        elif data['http_method'] == 'DELETE':
            log.info("DELETE to {}".format(obj_url))
            try:
                res = self.client.delete(obj_url)
            except Exception as e:
                log.exception("Unable to DELETE data on ES")
                return "Unable to DELETE data on ES".format(e), False

        elif data['http_method'] == 'PUT':
            log.info("PUT to {} with data {}".format(obj_url, obj_info))
            try:
                res = self.client.put(obj_url, data=json.dumps(obj_info),
                                      headers={'Content-Type': 'application/json'})
            except Exception as e:
                log.exception("Unable to PUT data to ES")
                return "Unable to PUT data to ES".format(e), False
        else:
            return "Invalid http method", False

        # True or False to control the consumption of queue messages
        # Will only consume the message if the document was created
        # or removed successfully
        # On the case of a 404 on a delete, the message will be
        # consumed anyway, since the document doens't exist on ES
        if res.status_code in [200, 201] or \
           (data['http_method'] == 'DELETE' and res.status_code == 404):
            return "", True
        else:
            return "Object not created", False

    def _get_es_obj_url(self, obj_info):

        obj_id = '/'.join([obj_info.get('project_id'),
                           obj_info.get('container'),
                           obj_info.get('object')])

        return self.es_url + '/' + urllib.parse.quote_plus(obj_id)

    def get_alf_client(self):
        # alf is an OAuth 2 Client
        # https://github.com/globocom/alf
        alf = Client(
            token_endpoint=self.token_endpoint,
            client_id=self.client_id,
            client_secret=self.client_secret)

        return alf
