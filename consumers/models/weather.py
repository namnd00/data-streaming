"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        topic = message.topic()
        message_value = message.value()
        logger.info("weather process_message is incomplete - skipping")
        # TODO: Process incoming weather messages. Set the temperature and status.
        logger.info("Topic: {}".format(topic))
        logger.info("Message: {}".format(message_value))
        self.temperature = message_value['temperature']
        self.status = message_value['status']
        logger.debug("Weather with temperature: {} and status: {}"\
                     .format(self.temperature, self.status.replace("_", " ")))
