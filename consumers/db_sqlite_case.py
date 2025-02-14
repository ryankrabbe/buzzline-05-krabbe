""" db_sqlite_case.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

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

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    """
    logger.info("Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")

#####################################
# Define Function to Insert a Message Into the Database
#####################################       

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database.

    Args:
    - message (dict): Processed message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message["message"],
                    message["author"],
                    message["timestamp"],
                    message["category"],
                    message["sentiment"],
                    message["keyword_mentioned"],
                    message["message_length"],
                ),
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")

#####################################
# Define Function to Insert a Positive Message into the Database
#####################################


def insert_positive_sentiment_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a positive sentiment message into the SQLite database.

    Args:
    - message (dict): The positive sentiment message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Checking if message has positive sentiment before inserting into the database.")
    timestamp = message.get('timestamp')
    sentiment = message.get('sentiment')
    message_text = message.get('message')
    author = message.get('author')
    category = message.get('category')
    keyword_mentioned = message.get('keyword_mentioned')

    # Check if the sentiment is positive
    if sentiment > 0:  # Ensures only positive sentiment messages are inserted
        try:
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO streamed_messages (timestamp, sentiment, message, author, category, keyword_mentioned)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, (timestamp, sentiment, message_text, author, category, keyword_mentioned)
                )
                conn.commit()
            logger.info(f"Successfully inserted positive sentiment message: {message_text}")
        except Exception as e:
            logger.error(f"ERROR: Failed to insert positive sentiment message into the database: {e}")

#####################################
# Define Function to Retrieve Positive Sentiment Messages from the Database
#####################################

def get_positive_sentiment_data(db_path: pathlib.Path):
    """
    Retrieve all messages from the database with positive sentiment.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    Returns:
    - list: A list of messages with positive sentiment.
    """
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM streamed_messages WHERE sentiment > 0
                """
            )
            rows = cursor.fetchall()
            positive_sentiment_messages = []

            for row in rows:
                message = {
                    "id": row[0],
                    "message": row[1],
                    "author": row[2],
                    "timestamp": row[3],
                    "category": row[4],
                    "sentiment": row[5],
                    "keyword_mentioned": row[6],
                    "message_length": row[7],
                }
                positive_sentiment_messages.append(message)

            logger.info(f"Successfully retrieved {len(positive_sentiment_messages)} positive sentiment messages.")
            return positive_sentiment_messages
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve positive sentiment data from the database: {e}")
        return []


#####################################
# Define Function to Delete a Message from the Database
#####################################


def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")


#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.path = config.get_base_data_path
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "message": "I just shared a meme! It was amazing.",
        "author": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "category": "humor",
        "sentiment": 0.87,
        "keyword_mentioned": "meme",
        "message_length": 42,
    }

    insert_message(test_message, TEST_DB_PATH)

    # Retrieve the ID of the inserted test message
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                # Delete the test message
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
