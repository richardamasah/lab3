# Job 3: Load 3 sets of KPI data from S3 into DynamoDB

# Tables:
# - genre_kpis_table      ← genre_daily_stats/
# - top_songs_table       ← top_songs_per_genre/
# - top_genres_table      ← top_genres_per_day/

# Each item is inserted with proper primary keys.
# Float values are converted to Decimal to meet DynamoDB requirements.

import boto3
import pandas as pd
import pyarrow.parquet as pq
import os, glob, subprocess
from decimal import Decimal
import logging # Import the logging module

# Initialize logger for the Glue job
log = logging.getLogger(__name__)
log.setLevel(logging.INFO) # Set the logging level

# Base S3 folder for KPIs and local scratch directory
s3_base = "s3://lab3dynamo1/processed/kpis/"
local_base = "/tmp/kpi_data"

# Table-to-folder mapping with their handling functions
tables = {
    "genre_kpis_table": {
        "s3_folder": "genre_daily_stats",
        "handler": "handle_genre_kpis"
    },
    "top_songs_table": {
        "s3_folder": "top_songs_per_genre",
        "handler": "handle_top_songs"
    },
    "top_genres_table": {
        "s3_folder": "top_genres_per_day",
        "handler": "handle_top_genres"
    }
}

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")

def download_data(s3_folder):
    """
    Downloads all Parquet files from a given S3 folder to the local /tmp directory.
    """
    path = os.path.join(local_base, s3_folder)
    os.makedirs(path, exist_ok=True) # Create local directory if it doesn't exist
    s3_path = f"{s3_base}{s3_folder}/"
    log.info(f"Downloading data from {s3_path} to {path}")
    # Use aws s3 cp command for recursive download
    subprocess.run(["aws", "s3", "cp", s3_path, path, "--recursive"], check=True)
    return path

def read_parquet_files(folder):
    """
    Reads all Parquet files from the specified local folder into a single Pandas DataFrame.
    """
    files = glob.glob(folder + "/*.parquet")
    log.info(f"Found {len(files)} Parquet files in {folder}.")
    # Concatenate all Parquet files into one DataFrame
    return pd.concat([pq.read_table(f).to_pandas() for f in files])

def handle_genre_kpis(df, table_name):
    """
    Inserts genre-level KPIs from DataFrame into the specified DynamoDB table.
    Ensures data types are compatible with DynamoDB.
    """
    log.info(f"Starting write of genre KPIs to DynamoDB table: {table_name}")
    table = dynamodb.Table(table_name)
    success_count = 0

    # Iterate over each row and put item into DynamoDB
    for _, row in df.iterrows():
        try:
            item = {
                "date": str(row["date"]),
                "track_genre": str(row["track_genre"]),
                "listen_count": int(row["listen_count"]),
                "unique_listeners": int(row["unique_listeners"]),
                "total_listening_time_ms": int(row["total_listening_time_ms"]),
                "avg_listening_time_per_user_ms": Decimal(str(row["avg_listening_time_per_user_ms"])) # Convert float to Decimal
            }
            table.put_item(Item=item)
            success_count += 1
        except Exception as e:
            log.error(f"Failed to write row to {table_name}: {row.to_dict()} -> {e}", exc_info=True)

    log.info(f"Successfully inserted {success_count} genre KPI records into {table_name}.")

def handle_top_songs(df, table_name):
    """
    Inserts top songs data from DataFrame into the specified DynamoDB table.
    """
    log.info(f"Starting write of top songs to DynamoDB table: {table_name}")
    table = dynamodb.Table(table_name)
    success_count = 0

    for _, row in df.iterrows():
        try:
            item = {
                "date": str(row["date"]),
                "track_id": str(row["track_id"]), # Assuming track_id is present in this data
                "track_name": str(row["track_name"]),
                "track_genre": str(row["track_genre"]),
                "play_count": int(row["play_count"]),
                "rank": int(row["rank"])
            }
            table.put_item(Item=item)
            success_count += 1
        except Exception as e:
            log.error(f"Failed to write row to {table_name}: {row.to_dict()} -> {e}", exc_info=True)

    log.info(f"Successfully inserted {success_count} top songs records into {table_name}.")

def handle_top_genres(df, table_name):
    """
    Inserts top genres data from DataFrame into the specified DynamoDB table.
    """
    log.info(f"Starting write of top genres to DynamoDB table: {table_name}")
    table = dynamodb.Table(table_name)
    success_count = 0

    for _, row in df.iterrows():
        try:
            item = {
                "date": str(row["date"]),
                "track_genre": str(row["track_genre"]),
                "genre_listens": int(row["genre_listens"]),
                "rank": int(row["rank"])
            }
            table.put_item(Item=item)
            success_count += 1
        except Exception as e:
            log.error(f"Failed to write row to {table_name}: {row.to_dict()} -> {e}", exc_info=True)

    log.info(f"Successfully inserted {success_count} top genres records into {table_name}.")

# ========== MAIN EXECUTION ==========
log.info("Starting multi-table KPI DynamoDB loader job.")

# Ensure local_base directory exists for downloads
os.makedirs(local_base, exist_ok=True)

# Loop through each table configuration and process data
for table_name, meta in tables.items():
    log.info(f"--- Processing data for table: {table_name} ---")
    try:
        # Download data from S3 to local /tmp
        local_path = download_data(meta["s3_folder"])
        
        # Read downloaded Parquet files into a Pandas DataFrame
        df = read_parquet_files(local_path)
        log.info(f"Loaded {len(df)} records for {table_name}.")
        
        # Get the appropriate handler function by name and call it
        handler_function = globals()[meta["handler"]]
        handler_function(df, table_name)
        
    except Exception as e:
        # Log critical errors that prevent processing a table
        log.error(f"Fatal error encountered while processing {table_name}: {e}", exc_info=True)
        # In a Glue job, re-raising the exception will cause the job to fail.
        raise

log.info("All KPI tables processed successfully. Job complete.")

# Note: Python Shell jobs do not have a job.commit() equivalent from Glue PySpark
# The job's state is determined by the script's exit code.