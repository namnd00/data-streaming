"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

import models.constants as constants

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "schema.registry.url": constants.SCHEMA_REGISTRY_URL,
            "bootstrap.servers": constants.BOOTSTRAP_SERVERS,
            "group.id": constants.GROUP_ID,
            "session.timeout.ms": constants.SESSION_TIMEOUT_MS,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        admin_client = AdminClient({"bootstrap.servers": constants.BOOTSTRAP_SERVERS})

        # generate new topics
        generated_topics = admin_client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "auto.offset.reset": constants.AUTO_OFFSET_RESET,
                        "auto.create.topics.enable": constants.AUTO_CREATE_TOPICS_ENABLE,
                        "cleanup.policy": constants.CLEANUP_POLICY,
                        "compression.type": constants.COMPRESSION_TYPE,
                        "delete.retention.ms": constants.DELETE_RETENTION_MS,
                        "file.delete.delay.ms": constants.FILE_DELETE_DELAY_MS,
                    }
                )
            ]
        )

        for topic, data in generated_topics.items():
            try:
                data.result()
                logger.info(f"Topic {topic} created")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
                logger.info("Topic creation kafka integration incomplete - skipping")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO: Write cleanup code for the Producer here
        if self.producer is not None:
            try:
                self.producer.flush()
                self.producer.close()
            except Exception as e:
                logger.info("Producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
