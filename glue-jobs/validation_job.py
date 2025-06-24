# Job 1: Ingest + Validate + Clean + Join

# This job performs the initial data processing steps:
# - Loads raw user, song, and stream data from Glue Catalog.
# - Validates essential schema requirements.
# - Cleans rows with nulls or invalid values.
# - Logs and isolates bad records by category to a 'bad-records/' S3 folder.
# - Joins all cleaned tables to create a unified dataset.
# - Writes the final cleaned and joined data to a 'validated/' S3 folder.

import sys
import logging # Import the logging module
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col # Import col for more flexible filtering

# Initialize logger for the Glue job
log = logging.getLogger(__name__)
log.setLevel(logging.INFO) # Set the logging level

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration for S3 paths and Glue Catalog database
database_name = "lab3"
validated_path = "s3://lab3dynamo1/validated"
bad_base_path = "s3://lab3dynamo1/bad-records"

# Define required columns for each input table
required_users = {'user_id', 'user_name', 'user_age', 'user_country', 'created_at'}
required_songs = {'track_id', 'track_name', 'track_genre', 'duration_ms'}
required_stream = {'user_id', 'track_id', 'listen_time'}

def check_columns(df, required, table_name):
    """
    Checks if all required columns exist in the given Spark DataFrame.
    Raises an exception if any required column is missing.
    """
    actual_columns = set(df.columns)
    missing_columns = required - actual_columns
    if missing_columns:
        log.error(f"Missing columns in {table_name}: {missing_columns}")
        raise Exception(f"Schema mismatch detected in {table_name}.")
    log.info(f"Schema for {table_name} is valid (all required columns present).")

try:
    log.info("Loading tables (users, songs, streams) from Glue Catalog.")
    # Load data from Glue Catalog tables into DynamicFrames
    users_dyf = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="users_csv")
    songs_dyf = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="songs_csv")
    stream_dyf = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="streams1_csv")

    # Convert DynamicFrames to Spark DataFrames for easier manipulation
    users_df = users_dyf.toDF()
    songs_df = songs_dyf.toDF()
    stream_df = stream_dyf.toDF()

    log.info("Validating schemas for loaded tables.")
    # Perform schema validation
    check_columns(users_df, required_users, "users_csv")
    check_columns(songs_df, required_songs, "songs_csv")
    check_columns(stream_df, required_stream, "streams1_csv")

    log.info("Filtering out rows with null values and separating bad records.")
    # Filter bad rows (nulls) and separate good/bad data for each table
    bad_users = users_df.filter("user_id IS NULL OR user_name IS NULL OR user_age IS NULL OR user_country IS NULL OR created_at IS NULL")
    good_users = users_df.dropna(subset=["user_id", "user_name", "user_age", "user_country", "created_at"])

    bad_songs = songs_df.filter("track_id IS NULL OR track_genre IS NULL OR duration_ms IS NULL")
    good_songs = songs_df.dropna(subset=["track_id", "track_genre", "duration_ms"])

    bad_streams = stream_df.filter("user_id IS NULL OR track_id IS NULL OR listen_time IS NULL")
    good_streams = stream_df.dropna(subset=["user_id", "track_id", "listen_time"])

    log.info("Joining stream and song data.")
    # Join stream data with song data using 'track_id'
    stream_songs_df = good_streams.join(good_songs, on="track_id", how="inner")

    log.info("Joining combined stream-song data with user data.")
    # Join the combined data with user data using 'user_id'
    joined_df = stream_songs_df.join(good_users, on="user_id", how="inner")

    log.info("Filtering out joined records with invalid numerical values (e.g., non-positive duration/age).")
    # Filter invalid numerical values (e.g., duration_ms <= 0 or user_age <= 0)
    bad_joined = joined_df.filter((col("duration_ms") <= 0) | (col("user_age") <= 0))
    clean_df = joined_df.filter((col("duration_ms") > 0) & (col("user_age") > 0))

    log.info("Writing cleaned and validated joined data to S3.")
    # Save the final cleaned and joined data to the 'validated/' S3 folder
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(clean_df, glueContext, "cleaned_dyf"),
        connection_type="s3",
        connection_options={"path": validated_path},
        format="parquet" # Using Parquet format for efficiency
    )

    log.info("Writing all categories of bad records to separate S3 folders.")

    # Write bad records to their respective S3 subfolders
    if bad_users.count() > 0:
        log.warning(f"Found {bad_users.count()} bad user records. Writing to {bad_base_path}/users/")
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_users, glueContext, "bad_users"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/users/"},
            format="parquet"
        )

    if bad_songs.count() > 0:
        log.warning(f"Found {bad_songs.count()} bad song records. Writing to {bad_base_path}/songs/")
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_songs, glueContext, "bad_songs"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/songs/"},
            format="parquet"
        )

    if bad_streams.count() > 0:
        log.warning(f"Found {bad_streams.count()} bad stream records. Writing to {bad_base_path}/streams/")
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_streams, glueContext, "bad_streams"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/streams/"},
            format="parquet"
        )

    if bad_joined.count() > 0:
        log.warning(f"Found {bad_joined.count()} bad joined records. Writing to {bad_base_path}/joined/")
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_joined, glueContext, "bad_joined"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/joined/"},
            format="parquet"
        )

    log.info("Job 1 finished successfully. Cleaned and bad data have been written.")

except Exception as e:
    # Log any unhandled exceptions and re-raise to fail the Glue job
    log.error(f"Job 1 failed: {e}", exc_info=True)
    raise

job.commit()