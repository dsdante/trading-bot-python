import logging
import sys

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.level = logging.INFO

debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error
critical = logger.critical
