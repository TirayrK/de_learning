#!/usr/bin/env python3
import json
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_schema():
    client = storage.Client()
    bucket = client.bucket("us-central1-my-composer-82fad1a3-bucket")
    blob = bucket.blob("schemas/schema.json")
    schema_config = json.loads(blob.download_as_text())

    type_mapping = {
        'string': StringType(),
        'long': LongType(),
        'integer': IntegerType()
    }

    fields = []
    for field_name, field_type in schema_config.items():
        fields.append(StructField(field_name, type_mapping[field_type], True))

    return StructType(fields)

def main():
    if len(sys.argv) != 4:
        logger.error("Usage: process_wiki_views.py <input_path> <output_path> <target_date>")
        logger.error("Example: process_wiki_views.py gs://bucket/landing gs://bucket/processed 2025-01-09")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    target_date = sys.argv[3]

    logger.info(f"Starting Wikipedia processing for date: {target_date}")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")

    spark = SparkSession.builder \
        .appName("WikipediaPageviewsProcessor") \
        .getOrCreate()

    schema = load_schema()

    input_wildcard = f"{input_path}/{target_date}/*.gz"
    logger.info(f"Reading files from: {input_wildcard}")

    try:
        df = spark.read.option("delimiter", " ").schema(schema).csv(input_wildcard)
        logger.info(f"Successfully loaded DataFrame from: {input_wildcard}")

        if df.rdd.isEmpty():
            raise ValueError("No data found in input files")

        total_records = df.count()
        logger.info(f"Total records: {total_records}")

    except Exception as e:
        logger.error(f"Error reading files from {input_wildcard}: {e}")
        logger.error("Checking what files exist...")
        raise

    if total_records == 0:
        logger.error("No records found. Check if files exist for the specified date.")
        spark.stop()
        sys.exit(1)

    english_df = df.filter((col("domain_code") == "en") | (col("domain_code") == "en.m"))
    english_records = english_df.count()
    logger.info(f"English Wikipedia records: {english_records}")

    if english_records == 0:
        logger.error("No English Wikipedia records found.")
        spark.stop()
        sys.exit(1)

    aggregated_df = english_df.groupBy("page_title").agg(spark_sum("count_views").alias("total_views"))
    top10_df = aggregated_df.orderBy(col("total_views").desc()).limit(10)
    logger.info("Top 10 English Wikipedia articles:")
    top10_df.show(10, truncate=False)

    renamed_df = top10_df.withColumnRenamed("page_title", "article_title")
    final_df = renamed_df.withColumn("processing_date", lit(target_date))

    output_full_path = f"{output_path}/{target_date}"
    logger.info(f"Writing results to: {output_full_path}")

    final_df.write.mode("overwrite").parquet(output_full_path)
    logger.info("Processing completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()