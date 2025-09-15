#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType


def main():
    if len(sys.argv) != 4:
        print("Usage: process_wiki_views.py <input_path> <output_path> <target_date>")
        print("Example: process_wiki_views.py gs://bucket/landing gs://bucket/processed 2025-01-09")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    target_date = sys.argv[3]

    print(f"Starting Wikipedia processing for date: {target_date}")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")

    spark = SparkSession.builder \
        .appName("WikipediaPageviewsProcessor") \
        .getOrCreate()

    schema = StructType([
        StructField("domain_code", StringType(), True),
        StructField("page_title", StringType(), True),
        StructField("count_views", LongType(), True),
        StructField("total_response_size", IntegerType(), True)
    ])

    input_wildcard = f"{input_path}/{target_date}/*.gz"
    print(f"Reading files from: {input_wildcard}")

    # Add this check after reading files
    try:
        df = spark.read.option("delimiter", " ").schema(schema).csv(input_wildcard)
        print(f"Successfully loaded DataFrame from: {input_wildcard}")

        # Check if any files were actually read
        if df.rdd.isEmpty():
            raise ValueError("No data found in input files")

        total_records = df.count()
        print(f"Total records: {total_records}")

    except Exception as e:
        print(f"Error reading files from {input_wildcard}: {e}")
        # List what files actually exist
        print("Checking what files exist...")
        raise

    if total_records == 0:
        print("ERROR: No records found. Check if files exist for the specified date.")
        spark.stop()
        sys.exit(1)

    english_df = df.filter((col("domain_code") == "en") | (col("domain_code") == "en.m"))
    english_records = english_df.count()
    print(f"English Wikipedia records: {english_records}")

    if english_records == 0:
        print("ERROR: No English Wikipedia records found.")
        spark.stop()
        sys.exit(1)

    aggregated_df = english_df.groupBy("page_title").agg(spark_sum("count_views").alias("total_views"))
    top10_df = aggregated_df.orderBy(col("total_views").desc()).limit(10)
    print("Top 10 English Wikipedia articles:")
    top10_df.show(10, truncate=False)

    renamed_df = top10_df.withColumnRenamed("page_title", "article_title")
    final_df = renamed_df.withColumn("processing_date", lit(target_date))

    output_full_path = f"{output_path}/{target_date}"
    print(f"Writing results to: {output_full_path}")

    final_df.write.mode("overwrite").parquet(output_full_path)
    print("Processing completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()