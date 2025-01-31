import logging
from colorama import Fore, Style

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


class ColoredFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT
    }

    def format(self, record):
        record.levelname = f'{self.COLORS.get(record.levelno, "")}{record.levelname}{Style.RESET_ALL}'
        return super().format(record)


class ColoredLogger(logging.Logger):
    FORMAT = '%(asctime)s: %(levelname)s -> %(message)s'
    DATE_FMT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, name='colorlog'):
        logging.Logger.__init__(self, name, logging.DEBUG)
        console = logging.StreamHandler()
        console.setFormatter(ColoredFormatter(self.FORMAT, datefmt=self.DATE_FMT))
        self.addHandler(console)


logger = ColoredLogger()
