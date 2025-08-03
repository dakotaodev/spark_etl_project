from pyspark.sql.functions import (
    col, explode, lit, size, current_timestamp,
    when, length, split, array_contains, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, TimestampType

def extract_fields(df):
    return df.select(
        col("id").alias("video_id"),
        col("createTime").cast("timestamp").alias("post_time"),
        col("desc").alias("caption"),
        col("video.duration").alias("duration_sec"),
        col("video.definition").alias("resolution"),
        col("video.volumeInfo.Loudness").alias("video_loudness"),
        col("music.title").alias("song_title"),
        col("music.authorName").alias("song_artist"),
        col("stats.playCount").alias("views"),
        col("stats.diggCount").alias("likes"),
        col("stats.commentCount").alias("comments"),
        col("stats.shareCount").alias("shares"),
        col("stats.collectCount").alias("saves"),
        col("duetEnabled").alias("duet_enabled"),
        col("stitchEnabled").alias("stitch_enabled"),
        col("isAd").alias("is_ad"),
        col("isPinnedItem").alias("is_pinned"),
        col("vqScore").cast("float").alias("vq_score"),
        col("textExtra").alias("hashtag_meta")
    )

def enrich_metrics(df):
    return (
        df.withColumn("engagement_rate", 
                      (col("likes") + col("comments") + col("shares") + col("saves")) / col("views"))
          .withColumn("views_per_hour",
                      col("views") / ((unix_timestamp(current_timestamp()) - unix_timestamp("post_time")) / 3600))
          .withColumn("hashtag_list",
                      when(size(col("hashtag_meta")) > 0, 
                           split(col("caption"), " "))  # crude parsing fallback
                      .otherwise(lit([])))
    )