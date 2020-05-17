import json
from datetime import datetime

from unittest.mock import Mock, patch

from twisted.internet import defer, reactor
from twisted.trial.unittest import TestCase

from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.protocols.sqs.configs import SQSConfig
from jasmin.protocols.sqs.service import SQS
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
        self.RouterPB_f.mt_routing_table.add(DefaultRoute(SmppClientConnector('abc')), 0)

        # Instanciate a SMPPClientManagerPB (a requirement for SQS)
        SMPPClientPBConfigInstance = SMPPClientPBConfig()
        SMPPClientPBConfigInstance.authentication = False
        clientManager_f = SMPPClientManagerPB(SMPPClientPBConfigInstance)

        with patch('jasmin.protocols.sqs.service.boto3') as boto:
            self.sqs = Mock()
            boto.client.return_value = self.sqs
            self.sqs.get_queue_url.return_value = {'QueueUrl': 'testurl'}

            config = SQSConfig()
            config.out_queue = 'test'
            config.retry_queue = 'test-retry'
            SQS().get(self.RouterPB_f, clientManager_f, config)

    def tearDown(self):
        self.RouterPB_f.cancelPersistenceTimer()


class AuthenticationTestCases(SQSTestCases):
    @defer.inlineCallbacks
    def test_send_normal(self):
        self.g1.enable()
        self.u1.enable()
        self.sqs.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.u1.username,
                    'password': 'correct',
                    'to': '06155423',
                    'content': 'anycontent'})
            }]
        }

        SQS().get().retrieveMessages()

        yield waitFor(1)

        self.assertEqual(self.sqs.send_message.call_count, 1)
        callArgs = json.loads(self.sqs.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], 'Failed to send SubmitSmPDU to [cid:abc]')

    @defer.inlineCallbacks
    def test_send_disabled_user(self):
        self.u1.disable()

        self.sqs.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.u1.username,
                    'password': 'correct',
                    'to': '06155423',
                    'content': 'anycontent'})
            }]
        }

        SQS().get().retrieveMessages()

        yield waitFor(1)

        self.assertEqual(self.sqs.send_message.call_count, 1)
        callArgs = json.loads(self.sqs.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], 'Failed to send SubmitSmPDU to [cid:abc]')

    @defer.inlineCallbacks
    def test_send_disabled_group(self):
        self.g1.disable()

        self.sqs.receive_message.return_value = {
            'Messages': [{
                'ReceiptHandle': 'abc123',
                'Body': json.dumps({
                    'username': self.u1.username,
                    'password': 'correct',
                    'to': '06155423',
                    'content': 'anycontent'})
            }]
        }

        SQS().get().retrieveMessages()

        yield waitFor(1)

        self.assertEqual(self.sqs.send_message.call_count, 1)
        callArgs = json.loads(self.sqs.send_message.call_args_list[0][1]['MessageBody'])
        self.assertEqual(callArgs['reason'], 'Failed to send SubmitSmPDU to [cid:abc]')


class SendTestCases(SQSTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_post_send_json_with_correct_args(self):
        response = yield self.web.post(b'send', json_data={'username': self.username,
                                                           'password': 'incorrec',
                                                           'to': '06155423',
                                                           'content': 'anycontent'}, headers={b'Content-type': [b'application/json']})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(
        ), ("Error \"Authentication failure for username:%s\"" % self.username).encode())

    @defer.inlineCallbacks
    def test_post_send_json_with_extra_args(self):
        response = yield self.web.post(b'send', json_data={'username': self.username,
                                                           'password': 'incorrec',
                                                           'to': '06155423',
                                                           'content': 'anycontent',
                                                           'custom_tlvs': [
                                                               (0x3000, None,
                                                                'COctetString', 'test1234')
                                                           ]}, headers={b'Content-type': [b'application/json']})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(
        ), ("Error \"Authentication failure for username:%s\"" % self.username).encode())

    @defer.inlineCallbacks
    def test_post_send_json_with_auth_success(self):
        response = yield self.web.post(b'send', json_data={'username': self.username,
                                                           'password': 'correct',
                                                           'to': '06155423',
                                                           'content': 'anycontent',
                                                           'custom_tlvs': [
                                                               (0x3000, None,
                                                                'COctetString', 'test1234')
                                                           ]}, headers={b'Content-type': [b'application/json']})
        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

    @defer.inlineCallbacks
    def test_send_with_correct_args(self):
        response = yield self.web.post(b'send', {b'username': self.username,
                                                 b'password': 'incorrec',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(
        ), b"Error \"Authentication failure for username:%s\"" % self.username.encode())

    @defer.inlineCallbacks
    def test_send_with_incorrect_args(self):
        response = yield self.web.post(b'send', {b'username': self.username,
                                                 b'passwd': b'correct',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(
            response.value(), b"Error \"Mandatory argument [password] is not found.\"")

    @defer.inlineCallbacks
    def test_send_with_auth_success(self):
        response = yield self.web.post(b'send', {b'username': self.username,
                                                 b'password': b'correct',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

    @defer.inlineCallbacks
    def test_send_with_priority(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # Priority definitions
        valid_priorities = {0, 1, 2, 3}

        for params['priority'] in valid_priorities:
            response = yield self.web.post(b'send', params)
            self.assertEqual(response.responseCode, 500)

            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Priority definitions
        invalid_priorities = {-1, 'a', 44, 4}

        for params['priority'] in invalid_priorities:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             ('Error "Argument [priority] has an invalid value: [%s]."' % params['priority']).encode())

    @defer.inlineCallbacks
    def test_send_with_validity_period(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # Validity period definitions
        valid_vps = {0, 1, 2, 3, 4000}

        for params[b'validity-period'] in valid_vps:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Validity period definitions
        invalid_vps = {-1, 'a', 1.0}

        for params[b'validity-period'] in invalid_vps:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            # This is a normal error since SMPPClientManagerPB is not really running
            self.assertEqual(response.value(),
                             ('Error "Argument [validity-period] has an invalid value: [%s]."' % params[
                                 b'validity-period']).encode())

    @defer.inlineCallbacks
    def test_send_with_inurl_dlr(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

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

        for params['dlr-url'] in valid_urls:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # URL definitions
        invalid_urls = {'ftp://127.0.0.1/receipt',
                        'smtp://127.0.0.1:99/receipt',
                        'smpp://127.0.0.1/receipt',
                        '127.0.0.1:99',
                        'www.google.com',
                        'www.google.com:99/'}

        for params['dlr-url'] in invalid_urls:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value(),
                             b"Error \"Argument [dlr-url] has an invalid value: [%s].\"" % params['dlr-url'].encode())

    @defer.inlineCallbacks
    def test_send_without_args(self):
        response = yield self.web.post(b'send')
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value(),
                         b"Error \"Mandatory argument [to] is not found.\"")

    @defer.inlineCallbacks
    def test_send_with_some_args(self):
        response = yield self.web.post(b'send', {b'to': b'06155423', b'username': self.username})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(
            response.value(), b'Error \"Mandatory argument [password] is not found.\"')

    @defer.inlineCallbacks
    def test_send_with_tags(self):
        """Related to #455"""
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        valid_tags = {'12', '1,2', '1000,2,12123',
                      'a,b,c', 'A0,22,B4,4e', 'a-b,2'}
        for params['tags'] in valid_tags:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        invalid_tags = {';', '#,.,:', '+++,sh1t,3r='}
        for params['tags'] in invalid_tags:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value(
            ), ('Error "Argument [tags] has an invalid value: [%s]."' % params['tags']).encode())

    @defer.inlineCallbacks
    def test_send_hex_content(self):
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423'}

        # Assert having an error if content and hex_content are not present
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value(),
                         b"Error \"content or hex-content not present.\"")

        # Assert having an error if content and hex_content are present
        params['hex-content'] = b''
        params[b'content'] = b''
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.value(),
                         b"Error \"content and hex-content cannot be used both in same request.\"")

        # Assert correct encoding
        del (params[b'content'])
        params['hex-content'] = b''
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 500)
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Assert incorrect encoding
        params['hex-content'] = 'Clear text'
        response = yield self.web.post(b'send', params)

        self.assertEqual(response.responseCode, 400)
        self.assertEqual(response.value()[:33],
                         b"Error \"Invalid hex-content data: ")

    @defer.inlineCallbacks
    def test_send_with_sdt(self):
        """Related to #541"""
        params = {b'username': self.username,
                  b'password': b'correct',
                  b'to': b'06155423',
                  b'content': 'anycontent'}

        # Assert sdt optional
        response = yield self.web.post(b'send', params)
        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Assert valid sdt
        valid_sdt = {'000000000100000R'}
        for params['sdt'] in valid_sdt:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 500)
            self.assertEqual(response.value(),
                             b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

        # Assert invalid sdt
        invalid_sdt = {'', '000000000100000', '00', '00+',
                       '00R', '00-', '0000000001000000R', '00000000100000R'}
        for params['sdt'] in invalid_sdt:
            response = yield self.web.post(b'send', params)

            self.assertEqual(response.responseCode, 400)
            self.assertEqual(response.value()[:22], b"Error \"Argument [sdt] ")

    def setUp(self):
        SQSTestCases.setUp(self)

        # Provision Router with additional Users and Routes
        u2 = User(2, Group(2), 'user2', 'correct')
        u2.mt_credential.setQuota('balance', 100.2)
        u2.mt_credential.setQuota('submit_sm_count', 30)
        u3 = User(3, Group(2), 'user3', 'correct')
        u3.mt_credential.setQuota('balance', 10)
        self.RouterPB_f.users.append(u2)
        self.RouterPB_f.users.append(u3)

    @defer.inlineCallbacks
    def test_balance_with_correct_args(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'password': 'incorrec'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(json.loads(response.value()),
                         'Authentication failure for username:%s' % 'nathalie')

    @defer.inlineCallbacks
    def test_balance_with_incorrect_args(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'passwd': b'correct'})
        self.assertEqual(response.responseCode, 400)
        self.assertEqual(json.loads(response.value()),
                         'Mandatory argument [password] is not found.')

    @defer.inlineCallbacks
    def test_balance_with_auth_success_unlimited_quotas(self):
        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {
                         'balance': 'ND', 'sms_count': 'ND'})

    @defer.inlineCallbacks
    def test_balance_with_auth_success_defined_quotas_u2(self):
        response = yield self.web.get(b"balance", {b'username': 'user2',
                                                   b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {
                         'balance': 100.2, 'sms_count': 30})

    @defer.inlineCallbacks
    def test_balance_with_auth_success_defined_quotas_u3(self):
        response = yield self.web.get(b"balance", {b'username': 'user3',
                                                   b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(json.loads(response.value()), {
                         'balance': 10, 'sms_count': 'ND'})


class UserStatsTestCases(SQSTestCases):
    username = 'nathalie'

    @defer.inlineCallbacks
    def test_send_failure(self):
        # Save before
        _submit_sm_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().httpapi['submit_sm_request_count']

        response = yield self.web.post(b'send', {b'username': 'nathalie',
                                                 b'password': 'incorrec',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertNotEqual(response.responseCode, 200)
        self.assertEqual(_submit_sm_request_count + 0,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['submit_sm_request_count'])

    @defer.inlineCallbacks
    def test_send_success(self):
        # Save before
        _submit_sm_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().httpapi['submit_sm_request_count']

        response = yield self.web.post(b'send', {b'username': 'nathalie',
                                                 b'password': b'correct',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 500)
        self.assertEqual(_submit_sm_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['submit_sm_request_count'])

    @defer.inlineCallbacks
    def test_balance_failure(self):
        # Save before
        _balance_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().httpapi['balance_request_count']

        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'password': 'incorrec'})
        self.assertNotEqual(response.responseCode, 200)
        self.assertEqual(_balance_request_count + 0,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['balance_request_count'])

    @defer.inlineCallbacks
    def test_balance_success(self):
        # Save before
        _balance_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().httpapi['balance_request_count']

        response = yield self.web.get(b"balance", {b'username': 'nathalie',
                                                   b'password': b'correct'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(_balance_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['balance_request_count'])

    @defer.inlineCallbacks
    def test_rate_failure(self):
        # Save before
        _rate_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().httpapi['rate_request_count']

        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                                b'password': 'incorrec',
                                                b'to': b'06155423'})
        self.assertNotEqual(response.responseCode, 200)
        self.assertEqual(_rate_request_count + 0,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['rate_request_count'])

    @defer.inlineCallbacks
    def test_rate_success(self):
        # Save before
        _rate_request_count = self.RouterPB_f.getUser(
            1).getCnxStatus().httpapi['rate_request_count']

        response = yield self.web.get(b"rate", {b'username': 'nathalie',
                                                b'password': b'correct',
                                                b'to': b'06155423'})
        self.assertEqual(response.responseCode, 200)
        self.assertEqual(_rate_request_count + 1,
                         self.RouterPB_f.getUser(1).getCnxStatus().httpapi['rate_request_count'])


class StatsTestCases(SQSTestCases):
    username = 'nathalie'

    def setUp(self):
        SQSTestCases.setUp(self)

        # Re-init stats singleton collector
        created_at = HttpAPIStatsCollector().get().get('created_at')
        HttpAPIStatsCollector().get().init()
        HttpAPIStatsCollector().get().set('created_at', created_at)

    @defer.inlineCallbacks
    def test_send_with_auth_failure(self):
        stats = HttpAPIStatsCollector().get()

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

        response = yield self.web.post(b'send', {b'username': self.username,
                                                 b'password': 'incorrec',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})
        self.assertEqual(response.responseCode, 403)
        self.assertEqual(response.value(
        ), ("Error \"Authentication failure for username:%s\"" % self.username).encode())

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
        stats = HttpAPIStatsCollector().get()

        response = yield self.web.post(b'send', {b'username': self.username,
                                                 b'password': b'correct',
                                                 b'to': b'06155423',
                                                 b'content': 'anycontent'})

        self.assertEqual(response.responseCode, 500)
        # This is a normal error since SMPPClientManagerPB is not really running
        self.assertEqual(response.value(),
                         b"Error \"Cannot send submit_sm, check SMPPClientManagerPB log file for details\"")

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
