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

    logger.propagate = False
    logger.setLevel(logging.INFO)

    return logger
