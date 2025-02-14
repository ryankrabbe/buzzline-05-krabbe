"""
consumer_krabbe.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
import matplotlib.pyplot as plt
import pandas as pd

# import external modules
from kafka import KafkaConsumer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available
from consumers.db_sqlite_case import init_db, insert_message, insert_positive_sentiment_message, get_positive_sentiment_data

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_case import init_db, insert_message

#####################################
# Function to process a single message
# #####################################


def process_message(message: dict, db_path: pathlib.Path) -> dict:
    """
    Process and transform a single JSON message.
    Returns the processed message dictionary.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")

    try:
        # Safely retrieve sentiment and validate it
        sentiment = message.get("sentiment", None)
        if sentiment is not None:
            sentiment = float(sentiment)
        else:
            sentiment = 0.0

        # If sentiment is positive, store in the positive_sentiments table
        if sentiment > 0.5:
            logger.info(f"Positive sentiment message: {message}")
            insert_positive_sentiment_message(message, db_path) 

        # Build the processed message dictionary
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": sentiment,
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }

        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except ValueError as ve:
        logger.error(f"Value error processing message: {ve}")
    except KeyError as ke:
        logger.error(f"Missing key in message: {ke}")
    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}")
    return None
    
#####################################
# Visualize Sentiment Trends
#####################################

def visualize_sentiment(sqlite_path: pathlib.Path):
    """
    Using Matplotlib, visualize positive sentiment trends over time, grouping by seconds.
    """
    # Fetch positive sentiment messages
    positive_sentiment_data = get_positive_sentiment_data(sqlite_path)
    
    if not positive_sentiment_data:
        logger.warning("No positive sentiment data found.")
        return

    # Convert data into a pandas DataFrame
    df = pd.DataFrame(positive_sentiment_data, columns=["timestamp", "sentiment"])

    # Convert 'timestamp' to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Group by minutes
    df["timestamp_minutes"] = df["timestamp"].dt.floor("T")  # group by minute
    aggregated_data = df.groupby("timestamp_minutes")["sentiment"].mean().reset_index()

    # Apply rolling mean for smoothing (adjust window size as needed)
    aggregated_data["smoothed_sentiment"] = aggregated_data["sentiment"].rolling(window=5, min_periods=1).mean()

    # Plot the data
    plt.figure(figsize=(12, 8), facecolor='#f4f4f9')
    plt.plot(aggregated_data["timestamp_minutes"], aggregated_data["smoothed_sentiment"], marker="s", markeredgewidth=2,
             color="#4B9CD3", label="Smoothed Avg Sentiment", linestyle='--', linewidth=2, markersize=8)
    plt.title('Positive Sentiment Trend by Minutes', fontsize=18, fontweight='bold', color='#333333')
    plt.xlabel('Timestamp (Minutes)')
    plt.ylabel('Average Sentiment')
    plt.xticks(rotation=45, fontsize=12)
    plt.grid(True, which='both', linestyle=':', linewidth=0.8, color='gray')
    plt.legend(fontsize=12, loc='upper left', frameon=False)
    plt.tight_layout()
    plt.show()   


#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        # consumer is a KafkaConsumer
        # message is a kafka.consumer.fetcher.ConsumerRecord
        # message.value is a Python dictionary
        for message in consumer:
            processed_message = process_message(message.value, sql_path)
            if processed_message:
                insert_message(processed_message, sql_path)

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

    # visualize the sentiment trends
    visualize_sentiment(sqlite_path)


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
