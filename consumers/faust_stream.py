"""Defines trends calculations for stations"""
from pathlib import Path
from configparser import ConfigParser, ExtendedInterpolation

import logging

import faust


config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(f"{Path(__file__).parents[0].parents[0]}/kafka.ini")

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App(
    "stations-stream", broker=config['broker']['kafka.server'], store="memory://"
)

topic = app.topic(
    config['topics.consumers']['faust.station.source'],
    value_type=Station
)


out_topic = app.topic(
    config['topics.consumers']['faust.station.transformed'],
    partitions=1
)

table = app.Table(
    config['topics.consumers']['faust.station.transformed'],
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def station_stream(stations):
    async for station in stations:
        line = ''
        if station.red:
            line = 'red'
        elif station.blue:
            line = 'blue'
        elif station.green:
            line = 'green'

        transformed_station = TransformedStation(
            station.station_id,
            station.station_name,
            station.order,
            line
        )

        await out_topic.send(value=transformed_station)


if __name__ == "__main__":
    app.main()
