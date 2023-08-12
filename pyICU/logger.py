import logging
import colorlog


class CustomLogger:
    def __init__(self):
        self._logger: logging.Logger = logging.getLogger()  # Get the root logger
        self._logger.setLevel(logging.INFO)  # Set your desired log level

        formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(levelname)s:%(name)s:%(message)s%(reset)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        self._logger.addHandler(console_handler)

    def get_logger(self):
        return self._logger
