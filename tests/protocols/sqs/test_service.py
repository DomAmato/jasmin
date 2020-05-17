import json
from datetime import datetime

from unittest.mock import Mock, patch

from twisted.internet import defer, reactor
from twisted.trial.unittest import TestCase

from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.protocols.sqs.configs import SQSConfig
from jasmin.protocols.sqs.service import SQSService
from jasmin.protocols.sqs.stats import SQSStatsCollector
from jasmin.routing.Routes import DefaultRoute
from jasmin.routing.configs import RouterPBConfig
from jasmin.routing.jasminApi import User, Group, SmppClientConnector
from jasmin.routing.router import RouterPB


@defer.inlineCallbacks
def waitFor(seconds):
    # Wait seconds
    waitDeferred = defer.Deferred()
    reactor.callLater(seconds, waitDeferred.callback, None)
    yield waitDeferred


class SQSTestCases(TestCase):
    def setUp(self):
        # Instanciate a RouterPB (a requirement for SQS)
        RouterPBConfigInstance = RouterPBConfig()
        self.RouterPB_f = RouterPB(RouterPBConfigInstance)

        # Provision Router with User and Route
        self.g1 = Group(1)
        self.u1 = User(1, self.g1, 'nathalie', 'correct')
        self.RouterPB_f.groups.append(self.g1)
        self.RouterPB_f.users.append(self.u1)
        self.RouterPB_f.mt_routing_table.add(
            DefaultRoute(SmppClientConnector('abc')), 0)

        # Instanciate a SMPPClientManagerPB (a requirement for SQS)
        SMPPClientPBConfigInstance = SMPPClientPBConfig()
        SMPPClientPBConfigInstance.authentication = False
        clientManager_f = SMPPClientManagerPB(SMPPClientPBConfigInstance)

        with patch('jasmin.protocols.sqs.service.boto3') as boto:
            self.boto = Mock()
            boto.client.return_value = self.boto
            self.boto.get_queue_url.return_value = {'QueueUrl': 'testurl'}

            config = SQSConfig()
            config.out_queue = 'test'
            config.retry_queue = 'test-retry'
            self.sqs = SQSService(self.RouterPB_f, clientManager_f, config)

    def tearDown(self):
        self.RouterPB_f.cancelPersistenceTimer()


class AuthenticationTestCases(SQSTestCases):
    @defer.inlineCallbacks
    def test_send_normal(self):
        # self.g1.enable()
        # self.u1.enable()
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.u1.username,
                    'password': 'correct',
                    'to': '123456789',
                    'content': 'anycontent1'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        # Because the full suite of services is not made it fails but with a different message
        self.assertEqual(callArgs['reason'],
                         'Failed to send SubmitSmPDU to [cid:abc]')

    @defer.inlineCallbacks
    def test_send_disabled_user(self):
        self.u1.disable()

        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.u1.username,
                    'password': 'correct',
                    'to': '123456789',
                    'content': 'anycontent2'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(
            callArgs['reason'], 'Authentication failure for username:nathalie')

    @defer.inlineCallbacks
    def test_send_disabled_group(self):
        self.g1.disable()

        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.u1.username,
                    'password': 'correct',
                    'to': '123456789',
                    'content': 'anycontent3'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(
            callArgs['reason'], 'Authentication failure for username:nathalie')


class SendTestCases(SQSTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_send_with_correct_args(self):
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.username,
                    'password': 'incorrect',
                    'to': '123456789',
                    'content': 'anycontent3'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(
            callArgs['reason'], 'Authentication failure for username:%s' % self.username)

    @defer.inlineCallbacks
    def test_send_with_auth_success(self):
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({'username': self.username,
                                    'password': 'correct',
                                    'to': '06155423',
                                    'content': 'anycontent',
                                    'custom_tlvs': [
                                        (0x3000, None,
                                         'COctetString', 'test1234')
                                    ]})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")

    @defer.inlineCallbacks
    def test_send_with_priority(self):
        params = {'username': self.username,
                  'password': 'correct',
                  'to': '06155423',
                  'content': 'anycontent'}

        # Priority definitions
        valid_priorities = {0, 1, 2, 3}

        index = 0
        for params['priority'] in valid_priorities:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")
            index += 1

        # Priority definitions
        invalid_priorities = {-1, 'a', 44, 4}

        for params['priority'] in invalid_priorities:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], 'Argument [priority] has an invalid value: [%s].' % params['priority'])
            index += 1

    @defer.inlineCallbacks
    def test_send_with_validity_period(self):
        params = {'username': self.username,
                  'password': 'correct',
                  'to': '06155423',
                  'content': 'anycontent'}

        # Validity period definitions
        valid_vps = {0, 1, 2, 3, 4000}

        index = 0
        for params['validity-period'] in valid_vps:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")
            index += 1

        # Validity period definitions
        invalid_vps = {-1, 'a', 1.0}

        for params['validity-period'] in invalid_vps:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], 'Argument [validity-period] has an invalid value: [%s].' % params[
                                 'validity-period'])
            index += 1

    @defer.inlineCallbacks
    def test_send_with_inurl_dlr(self):
        params = {'username': self.username,
                  'password': 'correct',
                  'to': '06155423',
                  'content': 'anycontent'}

        # URL definitions
        valid_urls = {'http://127.0.0.1/receipt',
                      'http://127.0.0.1:99/receipt',
                      'https://127.0.0.1/receipt',
                      'https://127.0.0.1:99/receipt',
                      'https://127.0.0.1/receipt.html',
                      'https://127.0.0.1:99/receipt.html',
                      'http://www.google.com/receipt',
                      'http://www.google.com:99/receipt',
                      'http://www.google.com/receipt.html',
                      'http://www.google.com:99/receipt.html',
                      'http://www.google.com/',
                      'http://www.google.com:99/',
                      'http://www.google.com',
                      'http://www.google.com:99'}

        index = 0
        for params['dlr-url'] in valid_urls:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1 + index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")
            index += 1

        # URL definitions
        invalid_urls = {'ftp://127.0.0.1/receipt',
                        'smtp://127.0.0.1:99/receipt',
                        'smpp://127.0.0.1/receipt',
                        '127.0.0.1:99',
                        'www.google.com',
                        'www.google.com:99/'}

        for params['dlr-url'] in invalid_urls:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Argument [dlr-url] has an invalid value: [%s]." % params['dlr-url'])
            index += 1

    @defer.inlineCallbacks
    def test_send_without_args(self):
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': '{}'
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Mandatory argument [to] is not found.")

    @defer.inlineCallbacks
    def test_send_with_some_args(self):
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({'to': '06155423', 'username': self.username})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], 'Mandatory argument [password] is not found.')

    @defer.inlineCallbacks
    def test_send_with_tags(self):
        """Related to #455"""
        params = {'username': self.username,
                  'password': 'correct',
                  'to': '06155423',
                  'content': 'anycontent'}

        valid_tags = {'12', '1,2', '1000,2,12123',
                      'a,b,c', 'A0,22,B4,4e', 'a-b,2'}
        index = 0
        for params['tags'] in valid_tags:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")
            index += 1

        invalid_tags = {';', '#,.,:', '+++,sh1t,3r='}
        for params['tags'] in invalid_tags:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], 'Argument [tags] has an invalid value: [%s].' % params['tags'])
            index += 1

    @defer.inlineCallbacks
    def test_send_hex_content(self):
        params = {'username': self.username,
                  'password': 'correct',
                  'to': '06155423'}

        # Assert having an error if content and hex_content are not present
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps(params)
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "content or hex-content not present.")

        # Assert having an error if content and hex_content are present
        params['hex-content'] = ''
        params['content'] = ''
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps(params)
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 2)
        callArgs = json.loads(self.boto.send_message.call_args_list[1][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "content and hex-content cannot be used both in same request.")

        # Assert correct encoding
        del (params['content'])
        params['hex-content'] = ''
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps(params)
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 3)
        callArgs = json.loads(self.boto.send_message.call_args_list[2][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")

        # Assert incorrect encoding
        params['hex-content'] = 'Clear text'
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps(params)
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 4)
        callArgs = json.loads(self.boto.send_message.call_args_list[3][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Invalid hex-content data: 'Clear text'")

    @defer.inlineCallbacks
    def test_send_with_sdt(self):
        """Related to #541"""
        params = {'username': self.username,
                  'password': 'correct',
                  'to': '06155423',
                  'content': 'anycontent'}

        # Assert sdt optional
        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps(params)
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")

        # Assert valid sdt
        index = 1
        valid_sdt = {'000000000100000R'}
        for params['sdt'] in valid_sdt:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")
            index += 1

        # Assert invalid sdt
        invalid_sdt = {'', '000000000100000', '00', '00+',
                       '00R', '00-', '0000000001000000R', '00000000100000R'}
        for params['sdt'] in invalid_sdt:
            self.boto.receive_message.return_value = {
                'Messages': [{
                    'ReceiptHandle': 'abc123',
                    'Body': json.dumps(params)
                }]
            }

            self.sqs.retrieveMessages()

            yield waitFor(0.2)

            self.assertEqual(self.boto.send_message.call_count, 1+index)
            callArgs = json.loads(self.boto.send_message.call_args_list[index][1]['MessageBody'])
            self.assertEqual(callArgs['reason'], "Argument [sdt] has an invalid value: [%s]." % params['sdt'] )
            index += 1

class UserStatsTestCases(SQSTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_send_failure(self):
        # Save before
        _submit_sm_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().sqs['submit_sm_request_count']

        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({'username': 'nathalie',
                                    'password': 'incorrec',
                                    'to': '06155423',
                                    'content': 'anycontent'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], 'Authentication failure for username:nathalie')
        self.assertEqual(_submit_sm_request_count,
                         self.RouterPB_f.getUser(1).getCnxStatus().sqs['submit_sm_request_count'])

    @defer.inlineCallbacks
    def test_send_success(self):
        # Save before
        _submit_sm_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().sqs['submit_sm_request_count']

        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({'username': 'nathalie',
                                    'password': 'correct',
                                    'to': '06155423',
                                    'content': 'anycontent'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)

        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")
        self.assertEqual(_submit_sm_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().sqs['submit_sm_request_count'])


class StatsTestCases(SQSTestCases):
    username = 'nathalie'

    def setUp(self):
        SQSTestCases.setUp(self)

        # Re-init stats singleton collector
        created_at = SQSStatsCollector().get().get('created_at')
        SQSStatsCollector().get().init()
        SQSStatsCollector().get().set('created_at', created_at)

    @defer.inlineCallbacks
    def test_send_with_auth_failure(self):
        stats = SQSStatsCollector().get()

        self.assertTrue(type(stats.get('created_at')) == datetime)
        self.assertEqual(stats.get('request_count'), 0)
        self.assertEqual(stats.get('last_request_at'), 0)
        self.assertEqual(stats.get('auth_error_count'), 0)
        self.assertEqual(stats.get('route_error_count'), 0)
        self.assertEqual(stats.get('throughput_error_count'), 0)
        self.assertEqual(stats.get('charging_error_count'), 0)
        self.assertEqual(stats.get('server_error_count'), 0)
        self.assertEqual(stats.get('success_count'), 0)
        self.assertEqual(stats.get('last_success_at'), 0)

        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({'username': self.username,
                                    'password': 'incorrec',
                                    'to': '06155423',
                                    'content': 'anycontent'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)
        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Authentication failure for username:%s" % self.username)

        self.assertTrue(type(stats.get('created_at')) == datetime)
        self.assertEqual(stats.get('request_count'), 1)
        self.assertTrue(type(stats.get('last_request_at')) == datetime)
        self.assertEqual(stats.get('auth_error_count'), 1)
        self.assertEqual(stats.get('route_error_count'), 0)
        self.assertEqual(stats.get('throughput_error_count'), 0)
        self.assertEqual(stats.get('charging_error_count'), 0)
        self.assertEqual(stats.get('server_error_count'), 0)
        self.assertEqual(stats.get('success_count'), 0)
        self.assertEqual(stats.get('last_success_at'), 0)

    @defer.inlineCallbacks
    def test_send_with_auth_success(self):
        stats = SQSStatsCollector().get()

        self.boto.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({'username': self.username,
                                    'password': 'correct',
                                    'to': '06155423',
                                    'content': 'anycontent'})
            }]
        }

        self.sqs.retrieveMessages()

        yield waitFor(0.2)
        self.assertEqual(self.boto.send_message.call_count, 1)
        callArgs = json.loads(self.boto.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], "Failed to send SubmitSmPDU to [cid:abc]")

        self.assertTrue(type(stats.get('created_at')) == datetime)
        self.assertEqual(stats.get('request_count'), 1)
        self.assertTrue(type(stats.get('last_request_at')) == datetime)
        self.assertEqual(stats.get('auth_error_count'), 0)
        self.assertEqual(stats.get('route_error_count'), 0)
        self.assertEqual(stats.get('throughput_error_count'), 0)
        self.assertEqual(stats.get('charging_error_count'), 0)
        self.assertEqual(stats.get('server_error_count'), 1)
        self.assertEqual(stats.get('success_count'), 0)
        self.assertEqual(stats.get('last_success_at'), 0)
