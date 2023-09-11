import logging
from rich.logging import RichHandler

rich_logger = logging.getLogger("rich")
rich_handler = RichHandler(markup=True, rich_tracebacks=True)
rich_logger.addHandler(rich_handler)


def init_logger(level="INFO"):
    rich_logger.setLevel(logging.INFO)
