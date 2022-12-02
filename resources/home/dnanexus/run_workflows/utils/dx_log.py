import logging

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
    logger.addHandler(DXLogHandler())
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    return logger
