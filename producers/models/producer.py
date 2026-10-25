"""Producer base-class providing common utilites and functionality"""
import logging
import os
import time
from pathlib import Path


from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

logger = logging.getLogger(__name__)


BROKER_URL = os.getenv("BROKER_URL", "PLAINTEXT://localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

SCHEMA_DIR = Path(__file__).parents[0] / "schemas"


def load_schema(file_name):
    """Reads an Avro schema as text, which is what AvroSerializer takes"""
    return (SCHEMA_DIR / file_name).read_text()


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    # One client and one connection are enough for every producer instance
    _schema_registry = None

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
            "bootstrap.servers": BROKER_URL,
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = KafkaProducer(self.broker_properties)

        registry = Producer.schema_registry()
        self.key_serializer = AvroSerializer(registry, key_schema)
        self.value_serializer = (
            AvroSerializer(registry, value_schema) if value_schema else None
        )

    @classmethod
    def schema_registry(cls):
        """Returns the shared Schema Registry client, creating it on first use"""
        if cls._schema_registry is None:
            cls._schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        return cls._schema_registry

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logging.info(f"checking topic {self.topic_name}")

        client = AdminClient(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
        )

        topic_metadata = client.list_topics(timeout=5)

        # Check if topic exists in client.list_topics
        if self.topic_name in topic_metadata.topics:
            logger.info(f"topic {self.topic_name} already exists")
            return
        else:
            logger.info(
                f"creating topic {self.topic_name} with {self.num_partitions} "
                f"partitions and {self.num_replicas} replicas"
            )

        # If not exists, create topic
        futures = client.create_topics(
        [
            NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas)
        ])


        for _topic, future in futures.items():
            try:
                future.result()
                logger.info(f"{self.topic_name} topic creation complete")
            except Exception as e:
                logger.fatal(f"unable to create topic {self.topic_name}, {e}")

    @staticmethod
    def _on_delivery(err, msg):
        """Reports delivery failures, which are otherwise silent"""
        if err is not None:
            logger.error(f"failed to deliver message to {msg.topic()}: {err}")

    def produce(self, key, value):
        """Emits an Avro event on this producer's topic"""
        self.producer.produce(
            topic=self.topic_name,
            key=self.key_serializer(
                key, SerializationContext(self.topic_name, MessageField.KEY)
            ),
            value=self.value_serializer(
                value, SerializationContext(self.topic_name, MessageField.VALUE)
            ),
            on_delivery=Producer._on_delivery,
        )
        # Serves the delivery callbacks queued so far without blocking
        self.producer.poll(0)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is not None:
            logger.debug("flushing producer.")
            self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
