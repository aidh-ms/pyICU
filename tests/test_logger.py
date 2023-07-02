import unittest
from io import StringIO
import logging
from ICUTSToolbox.logger import CustomLogger


class LoggerTestCase(unittest.TestCase):
    def setUp(self):
        self.logger = CustomLogger().get_logger()

        # Create a StringIO object to capture the logging output
        self.log_output = StringIO()
        self.log_handler = logging.StreamHandler(self.log_output)
        self.logger.addHandler(self.log_handler)

    def tearDown(self):
        self.logger.removeHandler(self.log_handler)
        self.log_output.close()

    def test_info_logging(self):
        message = "This is an info message"
        self.logger.info(message)
        log_output = self.log_output.getvalue().strip()

        self.assertIn(message, log_output)
        self.assertIn("info", log_output)

    def test_warning_logging(self):
        message = "This is a warning message"
        self.logger.warning(message)
        log_output = self.log_output.getvalue().strip()

        self.assertIn(message, log_output)
        self.assertIn("warning", log_output)

    def test_error_logging(self):
        message = "This is an error message"
        self.logger.error(message)
        log_output = self.log_output.getvalue().strip()

        self.assertIn(message, log_output)
        self.assertIn("error", log_output)


if __name__ == '__main__':
    unittest.main()
