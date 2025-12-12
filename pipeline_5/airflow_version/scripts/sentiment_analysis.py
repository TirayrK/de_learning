"""
Sentiment Analysis PySpark Job
Processes Amazon food reviews CSV files and generates sentiment scores
"""

import argparse
import logging
import time
import os
import nltk
from typing import List
from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    udf, col, count, when, avg, round as spark_round, lit, input_file_name, regexp_extract
)
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import pandas_udf
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def download_nltk_data_from_gcs(gcs_path: str, local_dir: str):
    """Downloads NLTK data from GCS to local directory"""
    logger.info(f"Downloading NLTK data from {gcs_path}")

    gcs_path_clean = gcs_path.replace('gs://', '')
    bucket_name = gcs_path_clean.split('/')[0]
    gcs_prefix = '/'.join(gcs_path_clean.split('/')[1:])

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_prefix)

    downloaded_count = 0
    for blob in blobs:
        if blob.name.endswith('/'):
            continue

        relative_path = os.path.relpath(blob.name, gcs_prefix)
        destination_file = os.path.join(local_dir, relative_path)

        os.makedirs(os.path.dirname(destination_file), exist_ok=True)
        blob.download_to_filename(destination_file)
        downloaded_count += 1

    logger.info(f"Downloaded {downloaded_count} NLTK files")
    return local_dir


def setup_nltk_data(nltk_gcs_path: str):
    """Download and configure NLTK data from GCS"""
    logger.info("Setting up NLTK data")

    if not nltk_gcs_path or not nltk_gcs_path.startswith('gs://'):
        raise ValueError(f"Invalid NLTK GCS path: {nltk_gcs_path}")

    local_nltk_dir = '/tmp/nltk_data'

    try:
        download_nltk_data_from_gcs(nltk_gcs_path, local_nltk_dir)
        nltk.data.path.insert(0, local_nltk_dir)

        vader_txt = os.path.join(local_nltk_dir, 'sentiment', 'vader_lexicon', 'vader_lexicon.txt')
        if not os.path.exists(vader_txt):
            raise FileNotFoundError(f"vader_lexicon.txt not found at {vader_txt}")

        logger.info("NLTK data setup complete")

    except Exception as e:
        logger.error(f"Failed to setup NLTK data: {e}")
        raise


class SparkConfig:
    """Spark session configuration"""

    @staticmethod
    def create_session(app_name: str) -> SparkSession:
        """Create and return Spark session"""
        logger.info("Initializing Spark session")

        try:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .getOrCreate()

            logger.info(f"Spark initialized (v{spark.version})")
            return spark
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise


class DataReader:
    """Handles reading CSV files and adding filename column"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_multiple_csvs(self, paths: List[str]) -> DataFrame:
        """Read multiple CSV files and add source_filename column"""
        logger.info(f"Reading {len(paths)} CSV file(s)")
        start = time.time()

        try:
            df = self.spark.read.csv(paths, header=True, inferSchema=True)

            df = df.withColumn(
                "source_filename",
                regexp_extract(input_file_name(), r'([^/]+)\.csv$', 1)
            )

            initial_count = df.count()
            df = df.filter(
                (col("Text").isNotNull() & (col("Text") != "")) |
                (col("Summary").isNotNull() & (col("Summary") != ""))
            )

            df.cache()
            row_count = df.count()
            removed_count = initial_count - row_count

            if row_count == 0:
                raise ValueError("No data found in input files")

            logger.info(f"Loaded {row_count:,} rows, removed {removed_count:,} rows without text ({time.time() - start:.1f}s)")

            return df

        except Exception as e:
            logger.error(f"Failed to read input files: {e}")
            raise


class SentimentAnalyzer:
    """Performs sentiment analysis using NLTK VADER"""

    def __init__(self, nltk_gcs_path: str):
        self.nltk_gcs_path = nltk_gcs_path

    @staticmethod
    def _classify_sentiment(score):
        """Classify sentiment score"""
        if score is None:
            return "No Text Review"
        elif score <= -0.7:
            return "Very Dissatisfied"
        elif score <= -0.1:
            return "Somewhat Dissatisfied"
        elif score < 0.1:
            return "Neutral"
        elif score < 0.7:
            return "Somewhat Satisfied"
        else:
            return "Very Satisfied"

    def analyze(self, df: DataFrame) -> DataFrame:
        """Apply sentiment analysis using pandas UDF"""
        logger.info("Performing sentiment analysis")
        start = time.time()

        nltk_path = self.nltk_gcs_path

        try:
            @pandas_udf(FloatType())
            def get_sentiment_score(text: pd.Series, summary: pd.Series) -> pd.Series:
                """Calculate sentiment scores for a batch"""
                from nltk.sentiment import SentimentIntensityAnalyzer

                local_nltk_dir = '/tmp/nltk_data'
                vader_file = os.path.join(local_nltk_dir, 'sentiment', 'vader_lexicon', 'vader_lexicon.txt')

                if not os.path.exists(vader_file):
                    gcs_path_clean = nltk_path.replace('gs://', '')
                    bucket_name = gcs_path_clean.split('/')[0]
                    gcs_prefix = '/'.join(gcs_path_clean.split('/')[1:])

                    storage_client = storage.Client()
                    bucket = storage_client.get_bucket(bucket_name)
                    blobs = bucket.list_blobs(prefix=gcs_prefix)

                    for blob in blobs:
                        if blob.name.endswith('/'):
                            continue
                        relative_path = os.path.relpath(blob.name, gcs_prefix)
                        destination_file = os.path.join(local_nltk_dir, relative_path)
                        os.makedirs(os.path.dirname(destination_file), exist_ok=True)
                        blob.download_to_filename(destination_file)

                nltk.data.path.insert(0, local_nltk_dir)
                sia = SentimentIntensityAnalyzer()

                def calculate_score(txt, summ):
                    combined = " ".join(filter(None, [str(summ or ""), str(txt or "")])).strip()
                    if not combined:
                        return None
                    try:
                        return float(sia.polarity_scores(combined)['compound'])
                    except:
                        return None

                return pd.Series([calculate_score(t, s) for t, s in zip(text, summary)])

            @pandas_udf(StringType())
            def classify_sentiment(scores: pd.Series) -> pd.Series:
                return scores.apply(SentimentAnalyzer._classify_sentiment)

            df_analyzed = df \
                .withColumn("sentiment_score", get_sentiment_score(col("Text"), col("Summary"))) \
                .withColumn("satisfaction_level", classify_sentiment(col("sentiment_score"))) \
                .withColumn("has_text_review", when(
                    (col("Text").isNotNull() & (col("Text") != "")) |
                    (col("Summary").isNotNull() & (col("Summary") != "")),
                    lit(1)).otherwise(lit(0))) \
                .withColumn("has_score_value", when(col("Score").isNotNull(), lit(1)).otherwise(lit(0)))

            df_analyzed.cache()
            row_count = df_analyzed.count()

            logger.info(f"Sentiment analysis complete ({time.time() - start:.1f}s)")
            return df_analyzed

        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            raise


class ProductAggregator:
    """Aggregates sentiment data by product and source file"""

    SATISFACTION_LEVELS = ["very_satisfied", "somewhat_satisfied", "neutral",
                          "somewhat_dissatisfied", "very_dissatisfied"]
    SCORES = [1, 2, 3, 4, 5]

    def aggregate(self, df: DataFrame) -> DataFrame:
        """Create product-level aggregation grouped by ProductId and source_filename"""
        logger.info("Creating product-level aggregation")
        start = time.time()

        try:
            agg_exprs = self._build_agg_expressions()

            product_agg = df.groupBy("source_filename", "ProductId").agg(*[
                expr.alias(name) for name, expr in agg_exprs.items()
            ])

            product_agg.cache()
            unique_groups = product_agg.count()
            logger.info(f"Aggregated {unique_groups:,} product-file combinations ({time.time() - start:.1f}s)")

            product_summary = self._calculate_percentages(product_agg)

            return product_summary

        except Exception as e:
            logger.error(f"Product aggregation failed: {e}")
            raise

    def _build_agg_expressions(self):
        """Build aggregation expressions"""
        agg_exprs = {
            "total_number_of_reviews": count("*"),
            "rows_without_text": count(when(col("has_text_review") == 0, 1)),
            "rows_without_score": count(when(col("has_score_value") == 0, 1)),
            "rows_with_text": count(when(col("has_text_review") == 1, 1)),
            "rows_with_score": count(when(col("has_score_value") == 1, 1)),
            "avg_sentiment_score": avg(when(col("sentiment_score").isNotNull(), col("sentiment_score"))),
            "avg_review_score": avg(when(col("Score").isNotNull(), col("Score"))),
        }

        for level in self.SATISFACTION_LEVELS:
            title_case = level.replace("_", " ").title()
            agg_exprs[f"{level}_count"] = count(
                when((col("satisfaction_level") == title_case) & (col("has_text_review") == 1), 1)
            )

        for score in self.SCORES:
            agg_exprs[f"score_{score}_count"] = count(
                when((col("Score") == score) & (col("has_score_value") == 1), 1)
            )

        return agg_exprs

    def _calculate_percentages(self, df: DataFrame) -> DataFrame:
        """Calculate percentage columns"""
        try:
            percentage_configs = [
                ("pct_without_text_review", "rows_without_text", "total_number_of_reviews", None, 2),
                ("pct_without_score_review", "rows_without_score", "total_number_of_reviews", None, 2),
            ] + [
                (f"{level}_pct", f"{level}_count", "rows_with_text", "rows_with_text", 2)
                for level in self.SATISFACTION_LEVELS
            ] + [
                (f"score_{score}_pct", f"score_{score}_count", "rows_with_score", "rows_with_score", 2)
                for score in self.SCORES
            ]

            result = df
            for new_col, count_col, denom_col, cond_col, decimals in percentage_configs:
                calc = spark_round((col(count_col) / col(denom_col)) * 100, decimals)
                result = result.withColumn(
                    new_col,
                    when(col(cond_col) > 0, calc).otherwise(lit(None)) if cond_col else calc
                )

            result = result \
                .withColumn("avg_sentiment_score", spark_round(col("avg_sentiment_score"), 4)) \
                .withColumn("avg_review_score", spark_round(col("avg_review_score"), 2)) \
                .select(
                    col("source_filename"),
                    col("ProductId").alias("product_id"),
                    "total_number_of_reviews", "avg_sentiment_score", "avg_review_score",
                    "pct_without_text_review", *[f"{l}_pct" for l in self.SATISFACTION_LEVELS],
                    "pct_without_score_review", *[f"score_{s}_pct" for s in [5, 4, 3, 2, 1]]
                )

            return result

        except Exception as e:
            logger.error(f"Percentage calculation failed: {e}")
            raise


class DataWriter:
    """Handles data writing to GCS"""

    @staticmethod
    def write_parquet(df: DataFrame, path: str, description: str):
        """Write dataframe to parquet"""
        logger.info(f"Saving {description} to {path}")
        start = time.time()

        try:
            df.write.mode("overwrite").parquet(path)
            logger.info(f"{description.capitalize()} saved ({time.time() - start:.1f}s)")
        except Exception as e:
            logger.error(f"Failed to save {description}: {e}")
            raise


class SentimentPipeline:
    """Main pipeline orchestrator"""

    def __init__(self, input_paths: List[str], reviews_output: str, summary_output: str, nltk_path: str):
        self.input_paths = input_paths
        self.reviews_output = reviews_output
        self.summary_output = summary_output
        self.nltk_path = nltk_path
        self.spark = SparkConfig.create_session("Amazon Reviews Sentiment Analysis")

    def run(self):
        """Execute the complete pipeline"""
        logger.info(f"Starting sentiment analysis pipeline")
        logger.info(f"Processing {len(self.input_paths)} file(s)")
        start_time = time.time()

        try:
            setup_nltk_data(self.nltk_path)

            reader = DataReader(self.spark)
            df = reader.read_multiple_csvs(self.input_paths)

            analyzer = SentimentAnalyzer(self.nltk_path)
            df_analyzed = analyzer.analyze(df)

            reviews_columns = ["source_filename", "Id", "ProductId", "UserId", "ProfileName", "HelpfulnessNumerator",
                             "HelpfulnessDenominator", "Score", "Time", "Summary", "Text",
                             "sentiment_score", "satisfaction_level"]
            DataWriter.write_parquet(
                df_analyzed.select(*reviews_columns),
                self.reviews_output,
                "detailed reviews"
            )

            aggregator = ProductAggregator()
            product_summary = aggregator.aggregate(df_analyzed)

            DataWriter.write_parquet(
                product_summary,
                self.summary_output,
                "product summary"
            )

            total_time = time.time() - start_time
            row_count = df.count()
            logger.info(f"Pipeline completed successfully in {total_time / 60:.1f}m")
            logger.info(f"Processing rate: {row_count / total_time:.0f} rows/second")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()


def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='Amazon Reviews Sentiment Analysis')
    parser.add_argument('--input-paths', required=True, help='Comma-separated input CSV paths (GCS)')
    parser.add_argument('--reviews-output', required=True, help='Reviews output path (GCS)')
    parser.add_argument('--summary-output', required=True, help='Summary output path (GCS)')
    parser.add_argument('--nltk-path', required=True, help='NLTK data path (GCS)')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    input_paths = [p.strip() for p in args.input_paths.split(',')]

    pipeline = SentimentPipeline(
        input_paths=input_paths,
        reviews_output=args.reviews_output,
        summary_output=args.summary_output,
        nltk_path=args.nltk_path
    )

    pipeline.run()