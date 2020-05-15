import sys
import threading
import logging
import re
import json
import pickle
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

import boto3
from twisted.internet import reactor, defer
from smpp.pdu.constants import priority_flag_value_map
from smpp.pdu.smpp_time import parse
from smpp.pdu.pdu_types import RegisteredDeliveryReceipt, RegisteredDelivery
import messaging.sms.gsm0338

from jasmin.routing.Routables import RoutableSubmitSm
from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.protocols.sqs.stats import SQSStatsCollector
from jasmin.protocols.sqs.errors import (RouteNotFoundError, ConnectorNotFoundError,
                     ChargingError, ThroughputExceededError, InterceptorNotSetError,
                     InterceptorNotConnectedError, InterceptorRunError)
from jasmin.protocols.errors import ArgsValidationError
from jasmin.protocols.sqs.validation import ArgsValidator, SQSCredentialValidator
from jasmin.protocols import hex2bin, authenticate_user, update_submit_sm_pdu
from jasmin.tools.singleton import Singleton

LOG_CATEGORY = "jasmin-connector-sqs"


class SQSConnector:
    def __init__(self, RouterPB, SMPPClientManagerPB, config, interceptor=None):
        self.SMPPClientManagerPB = SMPPClientManagerPB
        self.RouterPB = RouterPB
        self.interceptorpb_client = interceptor

        self.config = config
        
        # Setup stats collector
        self.stats = SQSStatsCollector().get()
        self.stats.set('created_at', datetime.now())

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if len(self.log.handlers) != 1:
            self.log.setLevel(config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=config.log_file, when=config.log_rotate)
            formatter = logging.Formatter(config.log_format, config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

        self.sqs = boto3.client('sqs',
                                  aws_access_key_id=config.key,
                                  aws_secret_access_key=config.secret,
                                  region_name=config.region)

        response = self.sqs.get_queue_url(QueueName=config.in_queue)
        self.in_queue_url = response['QueueUrl']

        if config.out_queue:
            response = self.sqs.get_queue_url(QueueName=config.out_queue)
            self.out_queue_url = response['QueueUrl']

        if config.retry_queue:
            response = self.sqs.get_queue_url(QueueName=config.retry_queue)
            self.retry_queue_url = response['QueueUrl']
        
        # opFactory is initiated with a dummy SMPPClientConfig used for building SubmitSm only
        self.opFactory = SMPPOperationFactory(long_content_max_parts=self.config.long_content_max_parts,
                                              long_content_split=self.config.long_content_split)

    def retrieveMessages(self):
        """
        /send request processing

        Note: This method MUST behave exactly like jasmin.protocols.smpp.factory.SMPPServerFactory.submit_sm_event
        """
        try:
            request = self.sqs.receive_message(QueueUrl=self.in_queue_url)
        except Exception as e:
            self.log.error("Error receiving message from queue: %s", str(e))
            raise
        if 'Messages' not in request:
            # There are no messages
            return

        self.log.debug("processing send request: %s", request['Messages'][0])

        self.stats.inc('request_count')
        self.stats.set('last_request_at', datetime.now())

        try:
            # Validation (must have almost the same params as /rate service)
            fields = {'to': {'optional': False, 'pattern': re.compile(r'^\+{0,1}\d+$')},
                      'from': {'optional': True},
                      'coding': {'optional': True, 'pattern': re.compile(r'^(0|1|2|3|4|5|6|7|8|9|10|13|14){1}$')},
                      'username': {'optional': False, 'pattern': re.compile(r'^.{1,16}$')},
                      'password': {'optional': False, 'pattern': re.compile(r'^.{1,16}$')},
                      # Priority validation pattern can be validated/filtered further more
                      # through HttpAPICredentialValidator
                      'priority': {'optional': True, 'pattern': re.compile(r'^[0-3]$')},
                      'sdt': {'optional': True,
                              'pattern': re.compile(r'^\d{2}\d{2}\d{2}\d{2}\d{2}\d{2}\d{1}\d{2}(\+|-|R)$')},
                      # Validity period validation pattern can be validated/filtered further more
                      # through HttpAPICredentialValidator
                      'validity-period': {'optional': True, 'pattern': re.compile(r'^\d+$')},
                      'dlr': {'optional': False, 'pattern': re.compile(r'^(yes|no)$')},
                      'dlr-url': {'optional': True, 'pattern': re.compile(r'^(http|https)\://.*$')},
                      # DLR Level validation pattern can be validated/filtered further more
                      # through HttpAPICredentialValidator
                      'dlr-level'   : {'optional': True, 'pattern': re.compile(r'^[1-3]$')},
                      'dlr-method'  : {'optional': True, 'pattern': re.compile(r'^(get|post)$', re.IGNORECASE)},
                      'tags'        : {'optional': True, 'pattern': re.compile(r'^([-a-zA-Z0-9,])*$')},
                      'content'     : {'optional': True},
                      'hex-content' : {'optional': True},
                      'custom_tlvs' : {'optional': True}}

            json_data = json.loads(request['Messages'][0]['Body'])
            message = {}
            message['ReceiptHandle'] = request['Messages'][0]['ReceiptHandle']
            for key, value in json_data.items():
                # Make the values look like they came from form encoding all surrounded by [ ]
                if isinstance(value, bytes):
                    value = value.decode()

                if isinstance(key, bytes):
                    key = key.decode()

                message[key] = [value]

            # If no custom TLVs present, defaujlt to an [] which will be passed down to SubmitSM
            if 'custom_tlvs' not in message:
                message['custom_tlvs'] = [[]]

            # Default coding is 0 when not provided
            if 'coding' not in message:
                message['coding'] = ['0']

            # Set default for undefined updated_message.arguments
            if 'dlr-url' in message or 'dlr-level' in message:
                message['dlr'] = ['yes']
            if 'dlr' not in message:
                # Setting DLR updated_message to 'no'
                message['dlr'] = ['no']

            # Set default values
            if message['dlr'][0] == 'yes':
                if 'dlr-level' not in message:
                    # If DLR is requested and no dlr-level were provided, assume minimum level (1)
                    message['dlr-level'] = [1]
                if 'dlr-method' not in message:
                    # If DLR is requested and no dlr-method were provided, assume default (POST)
                    message['dlr-method'] = ['POST']

            # DLR method must be uppercase
            if 'dlr-method' in message:
                message['dlr-method'][0] = message['dlr-method'][0].upper()

            # Make validation
            v = ArgsValidator(message, fields)
            v.validate()

            # Check if have content --OR-- hex-content
            # @TODO: make this inside UrlArgsValidator !
            if 'content' not in json_data and 'hex-content' not in json_data:
                raise ArgsValidationError("content or hex-content not present.")
            elif 'content' in json_data and 'hex-content' in json_data:
                raise ArgsValidationError("content and hex-content cannot be used both in same request.")

            # Continue routing in a separate thread
            reactor.callFromThread(self.route_routable, message=message)
        except Exception as e:
            self.log.error("Error: %s", e)

    def sendMessage(self, message):
        if self.out_queue_url:
            self.log.info('Sending message %s', message)
            try:
                self.sqs.send_message(QueueUrl=self.out_queue_url,
                                        MessageGroupId='1',
                                        MessageDeduplicationId=str(uuid.uuid4()),
                                        MessageBody=message)
            except Exception as e:
                self.log.error("Error sending message to queue: %s", str(e))
        else:
            self.log.error('Outbound queue is not configured')

    @defer.inlineCallbacks
    def route_routable(self, message):
        original_message = deepcopy(message)
        try:
            # Do we have a hex-content ?
            if 'hex-content' not in message:
                # Convert utf8 to GSM 03.38
                if message['coding'][0] == '0':
                    if isinstance(message['content'][0], bytes):
                        short_message = message['content'][0].decode().encode('gsm0338', 'replace')
                    else:
                        short_message = message['content'][0].encode('gsm0338', 'replace')
                    message['content'][0] = short_message

                else:
                    # Otherwise forward it as is
                    short_message = message['content'][0]
            else:
                # Otherwise convert hex to bin
                short_message = hex2bin(message['hex-content'][0])

            # Authentication
            user = authenticate_user(
                message['username'][0],
                message['password'][0],
                self.RouterPB,
                self.stats,
                self.log
            )

            # Update CnxStatus
            user.getCnxStatus().httpapi['connects_count'] += 1
            user.getCnxStatus().httpapi['submit_sm_request_count'] += 1
            user.getCnxStatus().httpapi['last_activity_at'] = datetime.now()

            # Build SubmitSmPDU
            SubmitSmPDU = self.opFactory.SubmitSM(
                source_addr=None if 'from' not in message else message['from'][0],
                destination_addr=message['to'][0],
                short_message=short_message,
                data_coding=int(message['coding'][0]),
                custom_tlvs=message['custom_tlvs'][0])
            self.log.debug("Built base SubmitSmPDU: %s", SubmitSmPDU)

            # Make Credential validation
            v = SQSCredentialValidator('Send', user, message, submit_sm=SubmitSmPDU)
            v.validate()

            # Update SubmitSmPDU by default values from user MtMessagingCredential
            SubmitSmPDU = v.updatePDUWithUserDefaults(SubmitSmPDU)

            # Prepare for interception then routing
            routedConnector = None  # init
            routable = RoutableSubmitSm(SubmitSmPDU, user)
            self.log.debug("Built Routable %s for SubmitSmPDU: %s", routable, SubmitSmPDU)

            # Should we tag the routable ?
            tags = []
            if 'tags' in message:
                tags = message['tags'][0].split(',')
                for tag in tags:
                    if isinstance(tag, bytes):
                        routable.addTag(tag.decode())
                    else:
                        routable.addTag(tag)
                    self.log.debug('Tagged routable %s: +%s', routable, tag)

            # Intercept
            interceptor = self.RouterPB.getMTInterceptionTable().getInterceptorFor(routable)
            if interceptor is not None:
                self.log.debug("RouterPB selected %s interceptor for this SubmitSmPDU", interceptor)
                if self.interceptorpb_client is None:
                    self.stats.inc('interceptor_error_count')
                    self.log.error("InterceptorPB not set !")
                    raise InterceptorNotSetError('InterceptorPB not set !')
                if not self.interceptorpb_client.isConnected:
                    self.stats.inc('interceptor_error_count')
                    self.log.error("InterceptorPB not connected !")
                    raise InterceptorNotConnectedError('InterceptorPB not connected !')

                script = interceptor.getScript()
                self.log.debug("Interceptor script loaded: %s", script)

                # Run !
                r = yield self.interceptorpb_client.run_script(script, routable)
                if isinstance(r, dict) and r['http_status'] != 200:
                    self.stats.inc('interceptor_error_count')
                    self.log.error('Interceptor script returned %s http_status error.', r['http_status'])
                    raise InterceptorRunError(
                        code=r['http_status'],
                        message='Interception specific error code %s' % r['http_status']
                    )
                elif isinstance(r, (str, bytes)):
                    self.stats.inc('interceptor_count')
                    routable = pickle.loads(r)
                else:
                    self.stats.inc('interceptor_error_count')
                    self.log.error('Failed running interception script, got the following return: %s', r)
                    raise InterceptorRunError(message='Failed running interception script, check log for details')

            # Get the route
            route = self.RouterPB.getMTRoutingTable().getRouteFor(routable)
            if route is None:
                self.stats.inc('route_error_count')
                self.log.error("No route matched from user %s for SubmitSmPDU: %s", user, routable.pdu)
                raise RouteNotFoundError("No route found")

            # Get connector from selected route
            self.log.debug("RouterPB selected %s route for this SubmitSmPDU", route)
            routedConnector = route.getConnector()
            # Is it a failover route ? then check for a bound connector, otherwise don't route
            # The failover route requires at least one connector to be up, no message enqueuing will
            # occur otherwise.
            if repr(route) == 'FailoverMTRoute':
                self.log.debug('Selected route is a failover, will ensure connector is bound:')
                while True:
                    c = self.SMPPClientManagerPB.perspective_connector_details(routedConnector.cid)
                    if c:
                        self.log.debug('Connector [%s] is: %s', routedConnector.cid, c['session_state'])
                    else:
                        self.log.debug('Connector [%s] is not found', routedConnector.cid)

                    if c and c['session_state'][:6] == 'BOUND_':
                        # Choose this connector
                        break
                    else:
                        # Check next connector, None if no more connectors are available
                        routedConnector = route.getConnector()
                        if routedConnector is None:
                            break

            if routedConnector is None:
                self.stats.inc('route_error_count')
                self.log.error("Failover route has no bound connector to handle SubmitSmPDU: %s", routable.pdu)
                raise ConnectorNotFoundError("Failover route has no bound connectors")

            # Re-update SubmitSmPDU with parameters from the route's connector
            connector_config = self.SMPPClientManagerPB.perspective_connector_config(routedConnector.cid)
            if connector_config:
                connector_config = pickle.loads(connector_config)
                routable = update_submit_sm_pdu(routable=routable, config=connector_config)

            # Set a placeholder for any parameter update to be applied on the pdu(s)
            param_updates = {}

            # Set priority
            priority = 0
            if 'priority' in message:
                priority = int(message['priority'][0])
                param_updates['priority_flag'] = priority_flag_value_map[priority]
            self.log.debug("SubmitSmPDU priority is set to %s", priority)

            # Set schedule_delivery_time
            if 'sdt' in message:
                param_updates['schedule_delivery_time'] = parse(message['sdt'][0])
                self.log.debug(
                    "SubmitSmPDU schedule_delivery_time is set to %s (%s)",
                    routable.pdu.params['schedule_delivery_time'],
                    message['sdt'][0])

            # Set validity_period
            if 'validity-period' in message:
                delta = timedelta(minutes=int(message['validity-period'][0]))
                param_updates['validity_period'] = datetime.today() + delta
                self.log.debug(
                    "SubmitSmPDU validity_period is set to %s (+%s minutes)",
                    routable.pdu.params['validity_period'],
                    message['validity-period'][0])

            # Got any updates to apply on pdu(s) ?
            if len(param_updates) > 0:
                routable = update_submit_sm_pdu(routable=routable, config=param_updates,
                                                config_update_params=list(param_updates))

            # Set DLR bit mask on the last pdu
            _last_pdu = routable.pdu
            while True:
                if hasattr(_last_pdu, 'nextPdu'):
                    _last_pdu = _last_pdu.nextPdu
                else:
                    break
            # DLR setting is clearly described in #107
            _last_pdu.params['registered_delivery'] = RegisteredDelivery(
                RegisteredDeliveryReceipt.NO_SMSC_DELIVERY_RECEIPT_REQUESTED)
            if message['dlr'][0] == 'yes':
                _last_pdu.params['registered_delivery'] = RegisteredDelivery(
                    RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED)
                self.log.debug(
                    "SubmitSmPDU registered_delivery is set to %s",
                    str(_last_pdu.params['registered_delivery']))

                dlr_level = int(message['dlr-level'][0])
                if 'dlr-url' in message:
                    dlr_url = message['dlr-url'][0]
                else:
                    dlr_url = None
                if message['dlr-level'][0] == '1':
                    dlr_level_text = 'SMS-C'
                elif message['dlr-level'][0] == '2':
                    dlr_level_text = 'Terminal'
                else:
                    dlr_level_text = 'All'
                dlr_method = message['dlr-method'][0]
            else:
                dlr_url = None
                dlr_level = 0
                dlr_level_text = 'No'
                dlr_method = None

            # QoS throttling
            if (user.mt_credential.getQuota('http_throughput') and user.mt_credential.getQuota('http_throughput') >= 0) and user.getCnxStatus().httpapi[
                'qos_last_submit_sm_at'] != 0:
                qos_throughput_second = 1 / float(user.mt_credential.getQuota('http_throughput'))
                qos_throughput_ysecond_td = timedelta(microseconds=qos_throughput_second * 1000000)
                qos_delay = datetime.now() - user.getCnxStatus().httpapi['qos_last_submit_sm_at']
                if qos_delay < qos_throughput_ysecond_td:
                    self.stats.inc('throughput_error_count')
                    self.log.error(
                        "QoS: submit_sm_event is faster (%s) than fixed throughput (%s), user:%s, rejecting message.",
                        qos_delay,
                        qos_throughput_ysecond_td,
                        user)

                    raise ThroughputExceededError("User throughput exceeded")
            user.getCnxStatus().httpapi['qos_last_submit_sm_at'] = datetime.now()

            # Get number of PDUs to be sent (for billing purpose)
            _pdu = routable.pdu
            submit_sm_count = 1
            while hasattr(_pdu, 'nextPdu'):
                _pdu = _pdu.nextPdu
                submit_sm_count += 1

            # Pre-sending submit_sm: Billing processing
            bill = route.getBillFor(user)
            self.log.debug("SubmitSmBill [bid:%s] [ttlamounts:%s] generated for this SubmitSmPDU (x%s)",
                           bill.bid, bill.getTotalAmounts(), submit_sm_count)
            charging_requirements = []
            u_balance = user.mt_credential.getQuota('balance')
            u_subsm_count = user.mt_credential.getQuota('submit_sm_count')
            if u_balance is not None and bill.getTotalAmounts() > 0:
                # Ensure user have enough balance to pay submit_sm and submit_sm_resp
                charging_requirements.append({
                    'condition': bill.getTotalAmounts() * submit_sm_count <= u_balance,
                    'error_message': 'Not enough balance (%s) for charging: %s' % (
                        u_balance, bill.getTotalAmounts())})
            if u_subsm_count is not None:
                # Ensure user have enough submit_sm_count to to cover
                # the bill action (decrement_submit_sm_count)
                charging_requirements.append({
                    'condition': bill.getAction('decrement_submit_sm_count') * submit_sm_count <= u_subsm_count,
                    'error_message': 'Not enough submit_sm_count (%s) for charging: %s' % (
                        u_subsm_count, bill.getAction('decrement_submit_sm_count'))})

            if self.RouterPB.chargeUserForSubmitSms(user, bill, submit_sm_count, charging_requirements) is None:
                self.stats.inc('charging_error_count')
                self.log.error('Charging user %s failed, [bid:%s] [ttlamounts:%s] SubmitSmPDU (x%s)',
                               user, bill.bid, bill.getTotalAmounts(), submit_sm_count)
                raise ChargingError('Cannot charge submit_sm, check RouterPB log file for details')

            ########################################################
            # Send SubmitSmPDU through smpp client manager PB server
            self.log.debug("Connector '%s' is set to be a route for this SubmitSmPDU", routedConnector.cid)
            c = self.SMPPClientManagerPB.perspective_submit_sm(
                uid=user.uid,
                cid=routedConnector.cid,
                SubmitSmPDU=routable.pdu,
                submit_sm_bill=bill,
                priority=priority,
                pickled=False,
                dlr_url=dlr_url,
                dlr_level=dlr_level,
                dlr_method=dlr_method,
                dlr_connector=routedConnector.cid)

            # Build final response
            if not c.result:
                self.stats.inc('server_error_count')
                raise Exception('Failed to send SubmitSmPDU to [cid:%s]', routedConnector.cid)
            else:
                self.stats.inc('success_count')
                self.stats.set('last_success_at', datetime.now())
                self.log.debug('SubmitSmPDU sent to [cid:%s], result = %s', routedConnector.cid, c.result)

            # Do not log text for privacy reasons
            # Added in #691
            if self.config.log_privacy:
                logged_content = b'** %d byte content **' % len(short_message)
            else:
                if isinstance(short_message, str):
                    short_message = short_message.encode()
                logged_content = b'%r' % re.sub(rb'[^\x20-\x7E]+', b'.', short_message)

            self.log.info(
                'SMS-MT [uid:%s] [cid:%s] [prio:%s] [dlr:%s] [from:%s] [to:%s] [content:%s]',
                user.uid,
                routedConnector.cid,
                priority,
                dlr_level_text,
                routable.pdu.params['source_addr'],
                message['to'][0],
                logged_content.decode())
        except Exception as e:
            self.log.error("Error: %s", e)
            if self.retry_queue_url:
                retry_message = {}
                for key, value in original_message.items():
                    if key != 'ReceiptHandle':
                        # message was mutated to make everything a list, unwind it and place it back on the queue
                        retry_message[key] = value[0]
                self.log.info('Sending message to the retry queue %s', retry_message)
                try:
                    self.sqs.send_message(QueueUrl=self.retry_queue_url,
                                            MessageGroupId='1',
                                            MessageDeduplicationId=str(uuid.uuid4()),
                                            MessageBody=json.dumps(retry_message))
                except Exception as e:
                    self.log.error("Error sending message to retry queue: %s", str(e))
        finally:
            if 'ReceiptHandle' not in original_message:
                self.log.error("Cannot delete message without ReceiptHandle")
                raise Exception("Cannot delete message without ReceiptHandle")
            try:
                self.sqs.delete_message(QueueUrl=self.in_queue_url,
                                        ReceiptHandle=original_message['ReceiptHandle'])
            except Exception as e:
                self.log.error("Error deleting message from queue. %s", str(e))
        
class SQS(metaclass=Singleton):
    """SQS connection holder"""
    sqs = None

    def get(self, RouterPB=None, SMPPClientManagerPB=None, config=None, interceptor=None):
        """Return a SQS's stats object or instanciate a new one"""
        if not self.sqs:
            self.sqs = SQSConnector(RouterPB, SMPPClientManagerPB, config, interceptor)

        return self.sqs
