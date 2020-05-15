# Copyright (c) Jookies LTD <jasmin@jookies.net>
# See LICENSE for details.

"""Jasmin SMS Gateway by Jookies LTD <jasmin@jookies.net>"""
import binascii
from jasmin.protocols.smpp.configs import SMPPClientConfig
from jasmin.protocols.errors import ArgsValidationError, AuthenticationError

def hex2bin(hex_content):
    """Convert hex-content back to binary data, raise a UrlArgsValidationError on failure"""

    try:
        return binascii.unhexlify(hex_content)
    except Exception as e:
        raise ArgsValidationError("Invalid hex-content data: '%s'" % hex_content)

def authenticate_user(username, password, routerpb, stats, log):
    if isinstance(username, bytes):
        username = username.decode()
    if isinstance(password, bytes):
        password = password.decode()

    user = routerpb.authenticateUser(
        username=username,
        password=password)
    if user is None:
        stats.inc('auth_error_count')

        log.debug(
            "Authentication failure for username:%s and password:%s",
            username, password)
        log.error(
            "Authentication failure for username:%s",
            username)
        raise AuthenticationError(
            'Authentication failure for username:%s' % username)
    return user

def update_submit_sm_pdu(routable, config, config_update_params=None):
    """Will set pdu parameters from smppclient configuration.
    Parameters that were locked through the routable.lockPduParam() method will not be updated.
    config parameter can be the connector config object or just a simple dict"""

    if config_update_params is None:
        # Set default config params to get from the config object
        config_update_params = [
            'protocol_id',
            'replace_if_present_flag',
            'dest_addr_ton',
            'source_addr_npi',
            'dest_addr_npi',
            'service_type',
            'source_addr_ton',
            'sm_default_msg_id',
        ]

    for param in config_update_params:
        _pdu = routable.pdu

        # Force setting param in main pdu
        if not routable.pduParamIsLocked(param):
            if isinstance(config, SMPPClientConfig) and hasattr(config, param):
                _pdu.params[param] = getattr(config, param)
            elif isinstance(config, dict) and param in config:
                _pdu.params[param] = config[param]

        # Force setting param in sub-pdus (multipart use case)
        while hasattr(_pdu, 'nextPdu'):
            _pdu = _pdu.nextPdu
            if not routable.pduParamIsLocked(param):
                if isinstance(config, SMPPClientConfig) and hasattr(config, param):
                    _pdu.params[param] = getattr(config, param)
                elif isinstance(config, dict) and param in config:
                    _pdu.params[param] = config[param]

    return routable