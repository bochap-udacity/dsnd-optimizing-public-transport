"""Configures KSQL to combine station and turnstile data"""
from pathlib import Path
from configparser import ConfigParser, ExtendedInterpolation

import json
import logging

import requests

import topic_check

config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(f"{Path(__file__).parents[0].parents[0]}/kafka.ini")

logger = logging.getLogger(__name__)


KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='{turnstile}',
    VALUE_FORMAT='avro',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
WITH (KAFKA_TOPIC='{turnstile_summary}', VALUE_FORMAT='json') AS
SELECT
    station_id, station_name, COUNT(station_id) AS count
FROM
    turnstile
GROUP BY station_id, station_name;
""".format(
    turnstile=config['topics.producers']['turnstile'],
    turnstile_summary=config['topics.consumers']['turnstile.summary']
)


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(config['topics.consumers']['turnstile.summary']) is True:
        logger.debug('Table creation has been completed')
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{config['ksql']['url']}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        # Ensure that a 2XX status code was returned
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"KSQL consumption error: {e.response.text}")


if __name__ == "__main__":
    execute_statement()
