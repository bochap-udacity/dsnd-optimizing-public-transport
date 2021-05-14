"""Producer base-class providing common utilites and functionality"""
from pathlib import Path
from configparser import ConfigParser, ExtendedInterpolation
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka import KafkaException

config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(
    f"{Path(__file__).parents[0].parents[0].parents[0]}/kafka.ini"
)
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
        self.broker_properties = {
            'schema.registry.url': config['broker']['schema.registry.url'],
            'bootstrap.servers': config['broker']['bootstrap.servers']
        }

        self.create_topic()

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({
            'bootstrap.servers': config['broker']['bootstrap.servers']
        })

        if len(Producer.existing_topics) == 0:
            topics = client.list_topics(timeout=10).topics
            for topic in topics.values():
                Producer.existing_topics.add(str(topic))

        if self.topic_name in Producer.existing_topics:
            return

        Producer.existing_topics.add(self.topic_name)

        futures = client.create_topics([
            NewTopic(
                self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas
            )
        ])

        for _, future in futures.items():
            try:
                future.result()
            except KafkaException as ke:
                logger.error(
                    f"topic creation error - {self.topic_name} - {ke}"
                )

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            self.producer.flush()
        except:
            logger.error(f"producer closing error")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
