"""
Config file handler for 'sqs' section in jasmin.cfg
"""

import logging
import os
from jasmin.config import ConfigFile

ROOT_PATH = os.getenv('ROOT_PATH', '/')
LOG_PATH = os.getenv('LOG_PATH', '%s/var/log/jasmin/' % ROOT_PATH)


class SQSConfig(ConfigFile):
    """Config handler for 'sqs' section"""

    def __init__(self, config_file=None):
        ConfigFile.__init__(self, config_file)

        self.in_queue = self._get('sqs', 'inbound_queue_name', 'jasmin')
        self.out_queue = self._get('sqs', 'outbound_queue_name', None)
        self.retry_queue = self._get('sqs', 'retry_queue_name', None)
        self.region = self._get('sqs', 'region', 'us-east-1')
        self.key = self._get('sqs', 'aws_key', None)
        self.secret = self._get('sqs', 'aws_secret', None)

        # Logging
        self.access_log = self._get(
            'sqs', 'access_log', '%s/http-accesslog.log' % LOG_PATH)
        self.log_level = logging.getLevelName(self._get('sqs', 'log_level', 'INFO'))
        self.log_file = self._get('sqs', 'log_file', '%s/sqs.log' % LOG_PATH)
        self.log_rotate = self._get('sqs', 'log_rotate', 'W6')
        self.log_format = self._get(
            'sqs', 'log_format', '%(asctime)s %(levelname)-8s %(process)d %(message)s')
        self.log_date_format = self._get('sqs', 'log_date_format', '%Y-%m-%d %H:%M:%S')
        self.log_privacy = self._getbool('sqs', 'log_privacy', False)

        # Long message splitting
        self.long_content_max_parts = self._get('sqs', 'long_content_max_parts', 5)
        self.long_content_split = self._get('sqs', 'long_content_split', 'udh')  # sar or udh
