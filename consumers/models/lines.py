"""Contains functionality related to Lines"""
from pathlib import Path
from configparser import ConfigParser, ExtendedInterpolation

import json
import logging

from models import Line

config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(f"{Path(__file__).parents[0].parents[0].parents[0]}/kafka.ini")

logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        topic = message.topic()
        if config['topics.producers']['station.prefix'] in topic:
            value = message.value()
            if message.topic() == config['topics.consumers']['faust.station.transformed']:
                value = json.loads(value)

            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif config['topics.consumers']['turnstile.summary'] == topic:
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", topic)
