"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

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

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        # README.md file & https://knowledge.udacity.com/questions/106575
        # 
        # Line: 184 SCHEMA_REGISTRY_URL
        # Line: 182 BROKER_URL
        self.broker_properties = {
            "SCHEMA_REGISTRY_URL" : "http://localhost:8081", # Schema Registry
            "BROKER_URL" : "PLAINTEXT://localhost:9092"

        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # Error forget use the self.broker_properties
        # used solution in Producing and Consuming Data with Schema Registry
        # CachedSchemaRegistryClient errors.
        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties["BROKER_URL"]},
            schema_registry=CachedSchemaRegistryClient({"url": self.broker_properties["SCHEMA_REGISTRY_URL"]},
                )
            )
        # Referenced: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroproducer-legacy
        # Parameter section. 

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        # Reference: exercise2.2.solution and https://knowledge.udacity.com/questions/385573
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#avroproducer-legacy
        # Refer parameters:
        # new_topics 
        # operation_timeout
        # request_timeout
        # num_partitions (int) – Number of partitions to create
        # exercise2.2.solution.py
        # https://knowledge.udacity.com/questions/385573
        client = AdminClient({"bootstrap.servers": self.broker_properties["BROKER_URL"]})
        
        topic_metadata = client.list_topics(timeout=5)
        
        futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=1,
                        replication_factor=1,
                        config={
                            "cleanup.policy": "delete",
                            "compression.type": "lz4",
                            "delete.retention.ms": "2000",
                            "file.delete.delay.ms": "2000",
                        },
                    )
                ]
            )
        for _, future in futures.items():
            try:
                futures.result()
                logger.info(f"created topic {self.topic_name}")
            except Exception as e:
                logger.info(f"failed to create topic {self.topic_name} {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        # Reference:
        # https://knowledge.udacity.com/questions/411861
        self.flush()
        # logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
