"""Defines trends calculations for stations"""
import logging

import faust
import constants


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


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App(
    constants.FAUST_APP_NAME,
    broker=constants.FAUST_BROKER_URL,
    store=constants.FAUST_STORE,
)
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic(constants.FAUST_STREAM_TOPIC, value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic(
    constants.FAUST_OUTPUT_TOPIC,
    partitions=constants.FAUST_OUTPUT_TOPIC_PARTITION,
)
# TODO: Define a Faust Table
table = app.Table(
    constants.FAUST_OUTPUT_TOPIC,
    default=TransformedStation,
    partitions=constants.FAUST_TABLE_PARTITION,
    changelog_topic=out_topic,
)

# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`

@app.agent(topic)
async def process(stream: faust.Stream[Station]):
    """Processes the input stream of `Station` records

    Args:
        stream (faust.Stream[Station]): the input stream of `Station` records
    """
    async for record in stream:
        logger.info(f"Processing record: {record}")
        line = None
        if record.red:
            line = "red"
        elif record.blue:
            line = "blue"
        else:
            line = "green"

        transformed_record = TransformedStation(
            station_id=record.station_id,
            station_name=record.station_name,
            order=record.order,
            line=line,
        )
        table[record.id] = transformed_record


if __name__ == "__main__":
    app.main()
