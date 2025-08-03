def write_to_delta(df, output_path="output/delta_tables/video_metrics"):
    (
        df.write.format("delta")
        .mode("overwrite")  # or "append"
        .partitionBy("resolution")
        .save(output_path)
    )