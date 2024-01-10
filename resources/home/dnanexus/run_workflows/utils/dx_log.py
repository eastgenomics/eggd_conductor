import json
import logging
import os

from dxpy import DXLogHandler


def dx_log() -> logging.Logger:
    """
    Set up logging to write to dx_stdout file for 'live' logging

    Returns
    -------
    logging.Logger
        logging handler object
    """
    logger = logging.getLogger(__name__)

    if os.path.exists('/opt/dnanexus/log/priority'):
        # running in DNAnexus, set up logging to monitor
        logger.addHandler(DXLogHandler())
        logger.setFormatter(CustomLoggingFormatter())

    logger.propagate = False
    logger.setLevel(logging.INFO)
    # log_format = logging.Formatter(json.dumps('$(message)s', indent='⠀⠀'))
    # logging.getLogRecordFactory()


    return logger

class CustomLoggingFormatter(logging.Formatter):
    data = {}

    def __init__(self):
        super(CustomLoggingFormatter, self).__init__()

    def format(self, record):
        print(record)
        record.message = record.getMessage()
        input_data = {}
        # input_data['@timestamp'] = datetime.utcnow().isoformat()[:-3] + 'Z'
        input_data['level'] = record.levelname

        if record.message:
            input_data['message'] = json.dumps(record.message, indent='⠀⠀')

        input_data.update(self.data)
        return json.dumps(input_data)
