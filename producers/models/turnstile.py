"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

from typing import Any
import models.constants as constants


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    """The turnstile data producer."""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        topic_name = constants.TURNSTILE_TOPIC_NAME if constants.TURNSTILE_TOPIC_NAME != "" \
            else f"{station_name}.turnstile"
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=constants.TURNSTILE_NUM_PARTIONS,
            num_replicas=constants.TURNSTILE_NUM_REPLICAS,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        if num_entries < 1:
            logger.warning("No entries found for timestamp %s", timestamp)
            return

        logger.info("Sending %d entries for timestamp %s", num_entries, timestamp)
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name,
                    },
                )
            except Exception as e:
                logger.error("Error sending turnstile data: %s", e)
