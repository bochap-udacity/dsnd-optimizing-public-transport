"""Configures a Kafka Connector for Postgres Station data"""
from pathlib import Path
from configparser import ConfigParser, ExtendedInterpolation

import json
import logging

import requests

logger = logging.getLogger(__name__)

config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(f"{Path(__file__).parents[0].parents[0]}/kafka.ini")

CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(
        f"{config['connect']['url']}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    resp = requests.post(
        config['connect']['url'],
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": config['postgres']['url'],
                "connection.user": config['postgres']['user'],
                "connection.password": config['postgres']['password'],
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": config['connect']['topic'],
                "poll.interval.ms": 10000,
            }
        }),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
        logging.debug("connector created successfully")
    except:
        logging.error(
            f"connector cannot be created: {json.dumps(resp.json(), indent=2)}")
        exit(1)


if __name__ == "__main__":
    configure_connector()
