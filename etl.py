import json
import logging
import os
import subprocess
from datetime import datetime
import sys
import boto3
from dotenv import load_dotenv
from logging_utils import configure_logging
from diskcache import Cache

# --- Configuration ---
load_dotenv()
DB_FILE = "nelo_analytics_rewrite.db"
# TODO: Replace with your actual SQS Queue URL from the AWS Console
QUEUE_URL = os.getenv("SQS_QUEUE_URL")
RAW_EVENTS_FILE = "raw_events.jsonl"
STAGING_DIR = "staging"
ARCHIVE_DIR = "archive"
LOG_FILE = "etl.log"

# --- Logging Setup ---
# Per-run log with stdout streaming for visibility
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
logger, _fmt = configure_logging(f"logs/etl_{RUN_TS}.log", logger_name="etl")
seen_msg_cache = Cache("seen_msg_cache")
MESSAGE_ID_TTL = 1209600 # 2 weeks retention same as SQS max retention.

def get_sqs_client():
    """Initializes and returns a boto3 SQS client."""
    try:
        sso_start_url = os.getenv("SSO_START_URL")
        sso_region = os.getenv("SSO_REGION")
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_session_token = os.getenv("AWS_SESSION_TOKEN")

        if not all(
            [
                sso_start_url,
                sso_region,
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
            ]
        ):
            logging.error("AWS credentials are not fully set in the environment.")
            raise ValueError("Missing AWS credentials in .env file")

        client = boto3.client(
            "sqs",
            region_name=sso_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
        logging.info("Successfully created SQS client.")
        return client
    except Exception as e:
        logging.error(f"Failed to create SQS client: {e}")
        return None


def consume_messages(sqs_client, num_messages_to_poll=10000):
    """Consumes a specified number of messages from SQS and appends them to a raw log file."""
    logging.info(f"Starting to poll for up to {num_messages_to_poll} messages.")
    messages_processed = 0

    while messages_processed < num_messages_to_poll:
        try:
            # Request a batch of 10 messages (the maximum)
            remaining = num_messages_to_poll - messages_processed
            batch_size = min(remaining, 10)

            response = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=batch_size,
                WaitTimeSeconds=5,  # Use long polling
            )

            messages = response.get("Messages", [])
            if not messages:
                logging.info("No more messages in the queue. Stopping consumption.")
                break

            with open(RAW_EVENTS_FILE, "a") as f:
                for msg in messages:
                    
                    # Check if message has already been processed in cache, to avoid deduplication compute downstream in db.
                    msg_id = msg["MessageId"]
                    if msg_id in seen_msg_cache:
                        logging.info(f"Message {msg_id} already processed. Skipping.")
                        continue
                    seen_msg_cache.add(msg_id, '1')

                    try:
                        f.write(msg["Body"] + "\n")
                        # No access to delete messages from SQS, however best practice is for upstream service to fan out via SNS
                        # not authorized to perform: sqs:deletemessage on resource: arn:aws:sqs:us-east-1:xxxxx:data-engineering-case-analytics-queue because no identity-based policy allows the sqs:deletemessage action
                        # sqs_client.delete_message(
                        #     QueueUrl=QUEUE_URL,
                        #     ReceiptHandle=msg['ReceiptHandle']
                        # )
                        messages_processed += 1

                    except Exception as e:
                        logging.error(f"Failed to process message: {e}")
                        

            logging.info(
                f"Consumed {len(messages)} messages. Total processed: {messages_processed}"
            )

        except Exception as e:
            logging.error(f"An error occurred while consuming messages: {e}")
            break

    logging.info(f"Finished polling. Total messages processed: {messages_processed}")
    return messages_processed > 0


def process_batch():
    """
    Processes the raw events file by renaming it and splitting events into
    separate files based on event_name.
    """
    if not os.path.exists(RAW_EVENTS_FILE) or os.path.getsize(RAW_EVENTS_FILE) == 0:
        logging.warning(
            "raw_events.jsonl does not exist or is empty. Nothing to process."
        )
        return False

    # 1. Initiate Batch: Atomically rename the raw file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_filename = f"processing_batch_{timestamp}.jsonl"
    os.rename(RAW_EVENTS_FILE, batch_filename)
    logging.info(f"Initiated batch: Renamed {RAW_EVENTS_FILE} to {batch_filename}")

    # Ensure directories exist
    os.makedirs(STAGING_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    # 2. Split and Stage Batch File
    logging.info(f"Splitting events from {batch_filename} into {STAGING_DIR}/")
    event_counts = {}
    try:
        with open(batch_filename, "r") as batch_file:
            for line in batch_file:
                try:
                    event = json.loads(line)
                    event_name = event.get("event_name", "unknown_event")

                    # Keep track of counts
                    event_counts[event_name] = event_counts.get(event_name, 0) + 1

                    # Append the raw line to the corresponding staging file
                    staging_filepath = os.path.join(STAGING_DIR, f"{batch_filename}")
                    with open(staging_filepath, "a") as staging_file:
                        staging_file.write(line)

                except json.JSONDecodeError:
                    logging.warning(f"Could not decode JSON from line: {line.strip()}")
                    # Optionally, write to a dead-letter log
                except Exception as e:
                    logging.error(
                        f"An unexpected error occurred processing line: {line.strip()} - {e}"
                    )

        logging.info("Successfully split batch file into event types.")
        for event_name, count in event_counts.items():
            logging.info(f"  - {event_name}: {count} events")

    except Exception as e:
        logging.error(f"Failed to process batch file {batch_filename}: {e}")
        # If splitting fails, we should rename the file back to allow reprocessing
        os.rename(batch_filename, RAW_EVENTS_FILE)
        logging.info(f"Rolled back: Renamed {batch_filename} back to {RAW_EVENTS_FILE}")
        return False

    # 5. Archive Batch (as per plan.md)
    archive_path = os.path.join(ARCHIVE_DIR, batch_filename)
    os.rename(batch_filename, archive_path)
    logging.info(f"Successfully archived batch file to {archive_path}")
    return True


if __name__ == "__main__":
    logging.info("--- Starting ETL Process ---")
    sqs = get_sqs_client()
    if sqs:
        # For this run, we consume and then immediately process.
        if consume_messages(sqs, num_messages_to_poll=1000):
            if process_batch():
                logging.info("Staging files created. Triggering processing script.")
                try:
                    env = os.environ.copy()
                    env["RUN_TS"] = RUN_TS
                    # Stream child logs live (no capture_output)
                    result = subprocess.run(
                        [sys.executable , "process_staging.py"], check=True, env=env
                    )
                    logging.info("Processing script completed successfully.")
                except FileNotFoundError:
                    logging.error(
                        "Processing script not found. Please ensure it exists."
                    )
                except subprocess.CalledProcessError as e:
                    logging.error(f"Processing script failed: {e}")
            else:
                logging.error("Batch processing failed. Staging files creation failed.")
        else:
            logging.info("No messages were consumed, skipping processing step.")
    logging.info("--- ETL Process Finished ---")
    # TODO: seen_msg_cache.close()
