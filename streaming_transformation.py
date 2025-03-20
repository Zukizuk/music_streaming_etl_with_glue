import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from decimal import Decimal

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_songs_path', 's3_users_path', 's3_streams_path', 'target_dynamodb_table'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data directly from S3
song_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("mode", "PERMISSIVE") \
    .csv(args['s3_songs_path'])

user_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(args['s3_users_path'])

merged_streams = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(args['s3_streams_path'])

# Select only needed columns
song_columns = ["track_id", "track_name", "duration_ms", "track_genre"]
user_columns = ["user_id"]
streams_columns = ["user_id", "track_id", "listen_time"]

song_df = song_df.select(song_columns)
user_df = user_df.select(user_columns)
merged_streams = merged_streams.select(streams_columns)

# Merge files for computation
merged_df = merged_streams.join(song_df, "track_id", "left").join(user_df, "user_id", "left")

# Clean data
merged_df = merged_df.na.drop()
merged_df = merged_df.dropDuplicates()

# Add listen_date from listen_time
merged_df = merged_df.withColumn("listen_date", merged_df["listen_time"].cast("date"))

# Calculate duration in minutes
merged_df = merged_df.withColumn("duration_minutes", merged_df["duration_ms"] / 60000)

# Compute daily genre metrics
daily_genre_metrics = merged_df.groupBy("listen_date", "track_genre").agg(
    F.count("*").alias("listen_count"),
    F.countDistinct("user_id").alias("unique_listeners"),
    F.round(F.sum("duration_minutes"), 2).alias("total_duration_minutes")
)

# Calculate Average Listening Time per User
daily_genre_metrics = daily_genre_metrics.withColumn(
    "avg_listening_time_per_user",
    F.round(F.col("total_duration_minutes") / F.col("unique_listeners"), 2)
)

# Calculate Top 3 Songs per Genre per Day
song_rank_window = Window.partitionBy("listen_date", "track_genre").orderBy(F.desc("track_listen_count"))

song_plays = merged_df.groupBy("listen_date", "track_genre", "track_name").agg(
    F.count("*").alias("track_listen_count")
)
song_plays_rank = song_plays.withColumn("rank", F.row_number().over(song_rank_window))
top_3_songs_per_genre = song_plays_rank.filter(F.col("rank") <= 3)

# Calculate Top 5 Genres per Day
genre_rank_window = Window.partitionBy("listen_date").orderBy(F.desc("listen_count"))
top_5_genres_per_day = daily_genre_metrics.withColumn("rank", F.row_number().over(genre_rank_window))
top_5_genres_per_day = top_5_genres_per_day.filter(F.col("rank") <= 5)

# Group top 3 songs
grouped_df_top3 = top_3_songs_per_genre.groupBy("listen_date", "track_genre").agg(
    F.collect_list("track_name").alias("top_3_songs")
)

# Group top 5 genres
grouped_df_top5 = top_5_genres_per_day.groupBy("listen_date").agg(
    F.collect_list("track_genre").alias("top_5_genres")
)

# Prepare data for DynamoDB using Glue's built-in writers
# Convert DataFrames to DynamicFrames
daily_metrics_dyf = DynamicFrame.fromDF(daily_genre_metrics, glueContext, "daily_metrics_dyf")
top3_songs_dyf = DynamicFrame.fromDF(grouped_df_top3, glueContext, "top3_songs_dyf")
top5_genres_dyf = DynamicFrame.fromDF(grouped_df_top5, glueContext, "top5_genres_dyf")

# Function to map data before writing to DynamoDB
def map_daily_genre_metrics(rec):
    rec["genre::date"] = f"{rec['track_genre']}::{rec['listen_date']}"
    rec["metric-type"] = "daily-genre-kpis"
    rec["genre"] = rec["track_genre"]
    return rec

def map_top3_songs(rec):
    rec["genre::date"] = f"{rec['track_genre']}::{rec['listen_date']}"
    rec["metric-type"] = "top-3-songs"
    rec["genre"] = rec["track_genre"]
    return rec

def map_top5_genres(rec):
    rec["genre::date"] = f"top5::{rec['listen_date']}"
    rec["metric-type"] = "top-5-genres"
    return rec

# Apply mappings
mapped_daily_metrics = Map.apply(frame=daily_metrics_dyf, f=map_daily_genre_metrics)
mapped_top3_songs = Map.apply(frame=top3_songs_dyf, f=map_top3_songs)
mapped_top5_genres = Map.apply(frame=top5_genres_dyf, f=map_top5_genres)

# Write to DynamoDB
glueContext.write_dynamic_frame.from_options(
    frame=mapped_daily_metrics,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": args['target_dynamodb_table'],
        "dynamodb.throughput.write.percent": "1.0"
    }
)

glueContext.write_dynamic_frame.from_options(
    frame=mapped_top3_songs,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": args['target_dynamodb_table'],
        "dynamodb.throughput.write.percent": "1.0"
    }
)

glueContext.write_dynamic_frame.from_options(
    frame=mapped_top5_genres,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": args['target_dynamodb_table'],
        "dynamodb.throughput.write.percent": "1.0"
    }
)

job.commit()