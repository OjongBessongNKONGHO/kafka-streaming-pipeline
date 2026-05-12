import json
import os
import signal
import sys
import yaml
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from src.utils.logger import get_logger
from consumer.processor import get_engine, process_message, save_to_dlq

load_dotenv()
logger = get_logger(__name__)

# Global flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """
    Handle SIGINT and SIGTERM signals for graceful shutdown.
    Sets the running flag to False so the consumer loop exits cleanly.
    """
    global running
    logger.info(f"Shutdown signal received ({signum}). Stopping consumer...")
    running = False


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_consumer(bootstrap_servers: str, config: dict) -> KafkaConsumer:
    """
    Create and return a KafkaConsumer with manual offset commits.
    
    Manual offset commits mean we only mark a message as processed
    AFTER it has been successfully written to PostgreSQL.
    If the consumer crashes mid-processing, the message will be
    redelivered on restart — no data loss.
    """
    return KafkaConsumer(
        config['kafka']['topic'],
        bootstrap_servers=bootstrap_servers,
        group_id=config['kafka']['consumer_group'],
        auto_offset_reset=config['kafka']['auto_offset_reset'],
        enable_auto_commit=False,  # Manual commits only
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        session_timeout_ms=config['kafka']['session_timeout_ms'],
        heartbeat_interval_ms=config['kafka']['heartbeat_interval_ms'],
        max_poll_records=config['kafka']['max_poll_records']
    )


def run_consumer():
    """
    Main consumer loop.
    
    Reads messages from Kafka topic continuously, processes each one,
    commits offsets manually only after successful processing,
    and handles failures by routing to the Dead Letter Queue.
    Shuts down gracefully on SIGINT or SIGTERM.
    """
    global running

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = load_config()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = config['kafka']['topic']
    dlq_topic = config['kafka']['dead_letter_topic']

    logger.info(f"Starting consumer | topic={topic} | group={config['kafka']['consumer_group']}")

    engine = get_engine()
    consumer = create_consumer(bootstrap_servers, config)

    messages_processed = 0
    messages_failed = 0

    try:
        while running:
            # Poll for new messages — timeout after 1 second
            # so we can check the running flag regularly
            records = consumer.poll(timeout_ms=1000)

            if not records:
                continue

            for partition, messages in records.items():
                for message in messages:
                    raw_message = message.value
                    offset = message.offset
                    msg_partition = message.partition
                    city = raw_message.get("city", "unknown")

                    logger.info(
                        f"Received | city={city} | "
                        f"partition={msg_partition} | "
                        f"offset={offset}"
                    )

                    # Process the message
                    success = process_message(
                        raw_message=raw_message,
                        kafka_offset=offset,
                        kafka_partition=msg_partition,
                        engine=engine
                    )

                    if success:
                        messages_processed += 1
                        # Commit offset only after successful processing
                        consumer.commit()
                    else:
                        messages_failed += 1
                        # Route failed message to Dead Letter Queue
                        save_to_dlq(
                            raw_message=json.dumps(raw_message),
                            error_type="ProcessingError",
                            error_detail="Failed during process_message()",
                            kafka_topic=topic,
                            kafka_offset=offset,
                            engine=engine
                        )
                        # Still commit offset to avoid infinite retry loop
                        consumer.commit()
                        logger.warning(
                            f"Failed message routed to DLQ | "
                            f"city={city} | offset={offset}"
                        )

    except KafkaError as e:
        logger.critical(f"Kafka error: {e}")
        sys.exit(1)

    finally:
        logger.info(
            f"Consumer shutting down | "
            f"processed={messages_processed} | "
            f"failed={messages_failed}"
        )
        consumer.close()
        logger.info("Consumer closed cleanly.")


if __name__ == "__main__":
    run_consumer()