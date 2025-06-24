# Job 2: Compute Daily Genre-Level KPIs from cleaned stream data

import sys
import logging # Import the logging module
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, count, countDistinct, sum, avg, row_number, to_date
from pyspark.sql.window import Window

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

# Define input and output S3 paths
input_path = "s3://lab3dynamo1/validated/"
output_base = "s3://lab3dynamo1/processed/kpis/"

try:
    log.info("Reading validated stream data from S3.")
    df = spark.read.parquet(input_path)

    log.info("Extracting date from listen_time column.")
    df = df.withColumn("date", to_date("listen_time"))

    # ========== 🎧 1. Genre-Level Daily KPIs ==========
    log.info("Calculating genre-level daily KPIs: listen count, unique listeners, total/average listening time.")

    # Group by date and genre to aggregate metrics
    genre_kpis = df.groupBy("date", "track_genre").agg(
        count("*").alias("listen_count"),
        countDistinct("user_id").alias("unique_listeners"),
        sum("duration_ms").alias("total_listening_time_ms")
    ).withColumn(
        "avg_listening_time_per_user_ms",
        # Calculate average listening time per user
        col("total_listening_time_ms") / col("unique_listeners")
    )

    # Write genre KPIs to S3
    genre_kpis.write.mode("overwrite").parquet(output_base + "genre_daily_stats/")
    log.info("Genre KPIs successfully written to genre_daily_stats/.")


    # ========== 🏆 2. Top 3 Songs per Genre per Day ===========
    log.info("Calculating top 3 songs per genre per day based on play counts.")

    # Aggregate play counts for each song per genre per day
    song_play_counts = df.groupBy("date", "track_genre", "track_name") \
        .agg(count("*").alias("play_count"))

    # Define window for ranking songs within each genre per day
    window_spec_song = Window.partitionBy("date", "track_genre").orderBy(col("play_count").desc())
    
    # Rank songs and filter for top 3
    top_songs = song_play_counts.withColumn("rank", row_number().over(window_spec_song)) \
        .filter(col("rank") <= 3)

    # Write top songs to S3
    top_songs.write.mode("overwrite").parquet(output_base + "top_songs_per_genre/")
    log.info("Top songs successfully written to top_songs_per_genre/.")

    # ========== 🔝 3. Top 5 Genres per Day ==========
    log.info("Calculating top 5 genres per day based on total listens.")

    # Aggregate total listens for each genre per day
    genre_counts = df.groupBy("date", "track_genre").agg(count("*").alias("genre_listens"))
    
    # Define window for ranking genres per day
    window_spec_genre = Window.partitionBy("date").orderBy(col("genre_listens").desc())

    # Rank genres and filter for top 5
    top_genres = genre_counts.withColumn("rank", row_number().over(window_spec_genre)) \
        .filter(col("rank") <= 5)

    # Write top genres to S3
    top_genres.write.mode("overwrite").parquet(output_base + "top_genres_per_day/")
    log.info("Top genres successfully written to top_genres_per_day/.")

    log.info("All KPIs computed and saved successfully.")

except Exception as e:
    log.error(f"KPI computation job failed: {e}", exc_info=True) # Log full traceback
    raise # Re-raise the exception to fail the Glue job

job.commit()