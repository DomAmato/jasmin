import json
from twisted.internet import defer, reactor
from datetime import datetime, timedelta
from tests.protocols.sqs.test_service import SQSTestCases


@defer.inlineCallbacks
def waitFor(seconds):
    # Wait seconds
    waitDeferred = defer.Deferred()
    reactor.callLater(seconds, waitDeferred.callback, None)
    yield waitDeferred


class CnxStatusCases(SQSTestCases):
    @defer.inlineCallbacks
    def test_connects_count(self):
        self.assertEqual(self.u1.getCnxStatus().sqs['connects_count'], 0)

        for i in range(10):
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

        yield waitFor(4)

        self.assertEqual(self.u1.getCnxStatus().sqs['connects_count'], 10)

    @defer.inlineCallbacks
    def test_last_activity_at(self):
        before_test = self.u1.getCnxStatus().sqs['last_activity_at']

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

        self.assertApproximates(datetime.now(),
                                self.u1.getCnxStatus().sqs['last_activity_at'],
                                timedelta(seconds=0.3))
        self.assertNotEqual(self.u1.getCnxStatus().sqs['last_activity_at'], before_test)

    @defer.inlineCallbacks
    def test_submit_sm_request_count(self):
        before_test = self.u1.getCnxStatus().sqs['submit_sm_request_count']

        for i in range(100):
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

        self.assertEqual(self.u1.getCnxStatus().sqs['submit_sm_request_count'], before_test + 100)
