import boto3
import logging
import time
from botocore.exceptions import ClientError
import subprocess # For the is_process_alive function

# --- Configuration and Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

s3_client = boto3.client('s3')
BUCKET_NAME = 'your-s3-bucket'
LOCK_KEY = 'locks/run_id_generator.lock'

# --- YARN Process Check Function ---
def is_process_alive(application_id: str) -> bool:
    if not application_id or not application_id.startswith('application_'):
        logging.warning(f"Invalid application_id format: {application_id}")
        return False
    command = ["yarn", "application", "-status", application_id]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return False
        for line in result.stdout.splitlines():
            if "State" in line and ":" in line:
                return "RUNNING" in line.split(":")[1].strip()
        return False
    except FileNotFoundError:
        logging.error("'yarn' command not found. Assuming process is alive to prevent incorrect lock stealing.")
        return True
    except Exception as e:
        logging.error(f"Error checking YARN status for {application_id}: {e}. Assuming alive.")
        return True

# --- Core Lock Acquisition Logic ---
def acquire_lock(my_app_id: str, max_wait_sec: int = 300, retry_delay_sec: int = 15) -> bool:
    """
    Acquires a distributed lock on S3 using a check-then-set pattern.

    Args:
        my_app_id: A unique identifier for the current process (e.g., Spark applicationId).
        max_wait_sec: Maximum time to wait for the lock.
        retry_delay_sec: Time to wait between retries.

    Returns:
        True if the lock was acquired, False otherwise.
    """
    start_time = time.time()
    lock_content = my_app_id.encode('utf-8')

    while time.time() - start_time < max_wait_sec:
        try:
            # 1. Check if the lock file exists using a lightweight HEAD request.
            s3_client.head_object(Bucket=BUCKET_NAME, Key=LOCK_KEY)
            
            # --- LOCK EXISTS ---
            logging.info("Lock file exists. Checking for staleness...")
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=LOCK_KEY)
            existing_app_id = response['Body'].read().decode('utf-8')

            if existing_app_id == my_app_id:
                logging.info("Lock already held by this process. Success.")
                return True

            if not is_process_alive(existing_app_id):
                logging.warning(f"Stale lock from dead process {existing_app_id} found. Attempting to steal.")
                # Overwrite the lock with our ID.
                s3_client.put_object(Bucket=BUCKET_NAME, Key=LOCK_KEY, Body=lock_content)
                # Read back to confirm we won a potential race to steal the lock.
                response = s3_client.get_object(Bucket=BUCKET_NAME, Key=LOCK_KEY)
                if response['Body'].read().decode('utf-8') == my_app_id:
                    logging.info(f"Successfully stole and acquired lock for {my_app_id}.")
                    return True
            else:
                logging.info(f"Lock held by active process {existing_app_id}. Waiting.")

        except ClientError as e:
            # --- LOCK DOES NOT EXIST ---
            if e.response['Error']['Code'] == '404':
                try:
                    logging.info("Lock file not found. Attempting to create.")
                    # Attempt to create the lock file.
                    s3_client.put_object(Bucket=BUCKET_NAME, Key=LOCK_KEY, Body=lock_content)
                    
                    # CRUCIAL: Read back immediately to verify we won the race.
                    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=LOCK_KEY)
                    written_id = response['Body'].read().decode('utf-8')
                    
                    if written_id == my_app_id:
                        logging.info(f"Lock successfully acquired by {my_app_id}.")
                        return True
                    else:
                        logging.warning(f"Lost race to create lock. Expected {my_app_id}, found {written_id}. Retrying.")
                except ClientError as put_e:
                    logging.error(f"Error during lock creation attempt: {put_e}")
            else:
                logging.error(f"An unexpected S3 error occurred: {e}")
        
        time.sleep(retry_delay_sec)

    raise TimeoutError(f"Could not acquire lock for {my_app_id} within {max_wait_sec} seconds.")

def release_lock(my_app_id: str):
    """Releases the lock only if it is still held by this process."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=LOCK_KEY)
        owner_id = response['Body'].read().decode('utf-8')
        if owner_id == my_app_id:
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=LOCK_KEY)
            logging.info(f"Lock released by {my_app_id}.")
        else:
            logging.warning(f"Did not release lock. Not the owner. Current owner: {owner_id}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.info("Lock already released (file not found).")
        else:
            logging.error(f"Error releasing lock: {e}")

# --- Example Usage in a Spark job ---
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# my_spark_app_id = spark.sparkContext.applicationId
#
# try:
#     if acquire_lock(my_spark_app_id):
#         # --- CRITICAL SECTION ---
#         print("Lock acquired. Proceeding with ID generation...")
#         # 1. Get max run_id from Hive
#         # 2. Calculate new run_id
#         # 3. Write new run_id back to Hive
#         # ...
# finally:
#     # Ensure the lock is always released
#     release_lock(my_spark_app_id)
