"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            "turnstile_events", # TODO: Come up with a better topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, # Uncomment once schema is defined
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        # logger.info("turnstile kafka integration incomplete - skipping")
        #
        # Refer https://knowledge.udacity.com/questions/567618
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.avro.AvroProducer
        # Parameters: timestamp (float, optional) – Message timestamp
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        # Incorrect syntax error: correct format
        # replace = with : and add {},instead ()
        # error with json. missing ,.
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name" : self.station.name,
                    "line": self.station.color.name
                    },
                )