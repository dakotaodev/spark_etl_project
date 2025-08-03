from src.transform.spark_session import get_spark_session
from src.transform.clean_and_parse import extract_fields, enrich_metrics
from src.load.write_to_delta import write_to_delta

def run_pipeline():
    spark = get_spark_session()

    # Load all JSONs from raw directory
    df_raw = spark.read.option("multiline", "true").json("data/raw/*.json")

    # Extract & clean
    df_clean = extract_fields(df_raw)
    df_enriched = enrich_metrics(df_clean)

    # Output to Delta
    write_to_delta(df_enriched)

    print("✅ TikTok ETL complete.")

if __name__ == "__main__":
    run_pipeline()