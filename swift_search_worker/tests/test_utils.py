import collections
import json
import unittest

from mock import Mock, patch
from swift_search_worker.utils import queue_connection, queue_channel,\
    get_obj_info, ElasticSearchUtils
from swift_search_worker.worker import callback


class UtilsTestCase(unittest.TestCase):

    def setUp(self):
        self.log = patch('swift_search_worker.utils.log', Mock()).start()

    def tearDown(self):
        patch.stopall()

    @patch('swift_search_worker.utils.pika')
    def test_queue_connection(self, mock_pika):

        mock_pika.PlainCredentials.return_value = 'credentials'
        mock_pika.ConnectionParameters.return_value = 'params'
        mock_pika.BlockingConnection.return_value = 'connection'

        computed = queue_connection(username='user',
                                    password='secret',
                                    host='rabbit.mq.com',
                                    vhost='vhost')

        mock_pika.PlainCredentials.assert_called_with('user', 'secret')

        mock_pika.ConnectionParameters.assert_called_with(
            host='rabbit.mq.com',
            port=5672,
            virtual_host='vhost',
            credentials='credentials')

        mock_pika.BlockingConnection.assert_called_with('params')

        self.log.debug.called_with('Queue connection OK')
        self.assertEqual(computed, 'connection')

    @patch('swift_search_worker.utils.pika')
    def test_queue_connection_fails(self, mock_pika):

        mock_pika.BlockingConnection.side_effect = Exception

        computed = queue_connection(username='user',
                                    password='secret',
                                    host='rabbit.mq.com',
                                    vhost='vhost')

        self.log.error.called_once()
        self.assertIsNone(computed)

    def test_queue_channel(self):

        connection = Mock()
        channel = Mock()
        connection.channel.return_value = channel

        computed = queue_channel(connection, 'queue_name')

        connection.channel.assert_called_once()
        channel.queue_declare.assert_called_with(queue='queue_name',
                                                 durable=True)

        self.assertEqual(computed, channel)

    def test_queue_channel_fails(self):

        connection = Mock()
        connection.channel.side_effect = Exception

        computed = queue_channel(connection, 'queue_name')

        self.log.exception.called_once()
        self.assertIsNone(computed)

    def test_get_obj_info(self):
        data = {
            'uri': '/v1/AUTH_acc/con/o/b/j',
            'headers': {
                'header1': 'value1',
                'header2': 'value2'
            },
            'timestamp': 'my-time-stamp'
        }

        computed = get_obj_info(data)
        expected = {
            'project_id': 'acc',
            'container': 'con',
            'object': 'o/b/j',
            'headers': data.get('headers'),
            'timestamp': data.get('timestamp')
        }

        self.assertEqual(computed, expected)

    def test_get_obj_info_invalid_uri(self):
        data = {
            'uri': '/healthcheck'
        }

        computed = get_obj_info(data)

        self.log.error.called_once()
        self.assertIsNone(computed)

    def test_get_obj_info_invalid_data(self):
        computed = get_obj_info(None)

        self.log.error.called_once()
        self.assertIsNone(computed)


class ElasticSearchUtilsTestCase(unittest.TestCase):

    def setUp(self):
        self.log = patch('swift_search_worker.utils.log', Mock()).start()

        # Dict similar to the message from queue
        self.data = {
            'http_method': 'POST',
            'uri': '/v1/acc/con/o/b/j',
            'headers': {
                'header1': 'value1',
                'header2': 'value2'
            },
            'timestamp': '2017-02-02T16:53:33.355817'
        }

        self.client = patch('swift_search_worker.utils.Client', Mock()).start()

        self.response = collections.namedtuple('Response', 'status_code')

    def tearDown(self):
        patch.stopall()

    # @patch('swift_search_worker.utils.Client')
    # def test_get_alf_client(self, mock):
    def test_get_alf_client(self):

        ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        self.client.assert_called_with(
            token_endpoint='token_url',
            client_id='cli_id',
            client_secret='cli_sec')

    def test_get_es_obj_url(self):
        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j'
        }

        computed = es._get_es_obj_url(obj_info)
        expected = 'es_url/1234%2Fcon%2Fo%2Fb%2Fj'

        self.assertEqual(computed, expected)

    def test_send_to_elastic_invalid_data(self):

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')
        msg, status = es.send_to_elastic({})

        self.assertFalse(status)
        self.assertEquals(msg, "Invalid object info")

    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_invalid_method(self, mock_get_obj_info):

        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')
        msg, status = es.send_to_elastic({'http_method': 'GET'})

        self.assertFalse(status)
        self.assertEquals(msg, "Invalid http method")

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_POST_method(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.post.return_value = self.response(status_code=200)

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')
        msg, status = es.send_to_elastic(self.data)

        client.post.assert_called_with(
            'obj-url',
            data=json.dumps(obj_info),
            headers={
                'Content-Type': 'application/json'
            })

        self.assertTrue(status)

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_ES_error(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.post.return_value = self.response(status_code=403)

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')
        msg, status = es.send_to_elastic(self.data)

        client.post.assert_called_with(
            'obj-url',
            data=json.dumps(obj_info),
            headers={
                'Content-Type': 'application/json'
            })

        self.assertFalse(status)
        self.assertEquals(msg, "Object not created")

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_POST_failed_connection(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.post.side_effect = Exception 

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        msg, status = es.send_to_elastic(self.data)
        
        client.post.assert_called_with(
            'obj-url',
            data=json.dumps(obj_info),
            headers={
                'Content-Type': 'application/json'
            })

        self.assertFalse(status)
        self.assertEqual(msg, 'Unable to POST data to ES')

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_DELETE_method(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.delete.return_value = self.response(status_code=200)

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        self.data['http_method'] = 'DELETE'
        _, status = es.send_to_elastic(self.data)

        client.delete.assert_called_with('obj-url')
        self.assertTrue(status)

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_DELETE_failed_connection(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.delete.side_effect = Exception 

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        self.data['http_method'] = 'DELETE'
        msg, status = es.send_to_elastic(self.data)

        client.delete.assert_called_with('obj-url')
        self.assertFalse(status)
        self.assertEqual(msg, 'Unable to DELETE data on ES')

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_DELETE_method_return_404(self,
                                                      mock_get_obj_info,
                                                      mock_url):

        client = self.client.return_value
        client.delete.return_value = self.response(status_code=404)

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        self.data['http_method'] = 'DELETE'
        _, status = es.send_to_elastic(self.data)

        client.delete.assert_called_with('obj-url')
        self.assertTrue(status)

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_PUT_method(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.put.return_value = self.response(status_code=201)

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        self.data['http_method'] = 'PUT'
        _, status = es.send_to_elastic(self.data)

        client.put.assert_called_with(
            'obj-url',
            data=json.dumps(obj_info),
            headers={
                'Content-Type': 'application/json'
            })

        self.assertTrue(status)

    @patch('swift_search_worker.utils.ElasticSearchUtils._get_es_obj_url')
    @patch('swift_search_worker.utils.get_obj_info')
    def test_send_to_elastic_PUT_failed_connection(self, mock_get_obj_info, mock_url):

        client = self.client.return_value
        client.put.side_effect = Exception

        # Mocked result from _get_es_obj_url
        mock_url.return_value = 'obj-url'

        # Mocked result from get_obj_info
        obj_info = {
            'project_id': '1234',
            'container': 'con',
            'object': 'o/b/j',
            'headers': {},
            'timestamp': '1997'
        }
        mock_get_obj_info.return_value = obj_info

        es = ElasticSearchUtils('token_url', 'cli_id', 'cli_sec', 'es_url')

        self.data['http_method'] = 'PUT'
        msg, status = es.send_to_elastic(self.data)

        client.put.assert_called_with(
            'obj-url',
            data=json.dumps(obj_info),
            headers={
                'Content-Type': 'application/json'
            })

        self.assertFalse(status)
        self.assertEqual(msg, 'Unable to PUT data to ES')


class WorkerTestCase(unittest.TestCase):

    def setUp(self):
        self.log = patch('swift_search_worker.worker.log', Mock()).start()

        # Dict similar to the message from queue
        self.data = json.dumps({
            'http_method': 'POST',
            'uri': '/v1/acc/con/o/b/j',
            'headers': {
                'header1': 'value1',
                'header2': 'value2'
            },
            'timestamp': '2017-02-02T16:53:33.355817'
        })
        
        self.method = collections.namedtuple('Method', 'delivery_tag')
        self.channel = Mock()
        
    def tearDown(self):
        patch.stopall()

    @patch('swift_search_worker.worker.elastic_utils')
    def test_callback_called_with_invalid_data(self, mock_elastic_utils):
        mock_elastic_utils.send_to_elastic.side_effect = ValueError
        method = self.method(delivery_tag='delivered') 
        callback(self.channel, method, None, self.data)
        self.log.error.called_with('Invalid message')

    @patch('swift_search_worker.worker.elastic_utils')
    def test_message_acknowledged(self, mock_elastic_utils):
        mock_elastic_utils.send_to_elastic.return_value = "", True
        method = self.method(delivery_tag='delivered') 
        callback(self.channel, method, None, self.data)
        self.channel.basic_ack.assert_called_with(delivery_tag='delivered')

    @patch('swift_search_worker.worker.elastic_utils')
    def test_message_failed(self, mock_elastic_utils):
        mock_elastic_utils.send_to_elastic.return_value = "generic error message", False
        method = self.method(delivery_tag='delivered') 
        callback(self.channel, method, None, self.data)
        self.log.error.called_with('Failed to create message on Elastic Search')

if __name__ == '__main__':
    unittest.main()
