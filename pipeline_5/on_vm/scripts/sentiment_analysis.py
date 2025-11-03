from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, when, avg, round as spark_round, lit
from pyspark.sql.types import StringType, FloatType
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("Starting Amazon Reviews Sentiment Analysis Pipeline")

try:
    nltk.data.find('vader_lexicon')
    logger.info("NLTK vader_lexicon found")
except LookupError:
    logger.warning("vader_lexicon not found, downloading...")
    try:
        nltk.download('vader_lexicon')
        logger.info("vader_lexicon downloaded successfully")
    except Exception as e:
        logger.error(f"Failed to download vader_lexicon: {e}")
        raise

logger.info("Initializing Spark session...")
start_time = time.time()
try:
    spark = SparkSession.builder \
        .appName("Amazon Reviews Sentiment Analysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    spark_init_time = time.time() - start_time
    logger.info(f"Spark initialized successfully (v{spark.version}, {spark_init_time:.1f}s)")
except Exception as e:
    logger.error(f"Failed to initialize Spark: {e}")
    raise

sia = SentimentIntensityAnalyzer()

input_path = "/opt/spark-data/Reviews.csv"
logger.info(f"Reading data from {input_path}")
read_start = time.time()

try:
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.cache()
    row_count = df.count()
    read_time = time.time() - read_start

    if row_count == 0:
        logger.error("Input file is empty")
        raise ValueError("No data to process")

    logger.info(f"Loaded {row_count:,} rows, {len(df.columns)} columns ({read_time:.1f}s)")

except FileNotFoundError:
    logger.error(f"Input file not found: {input_path}")
    raise
except Exception as e:
    logger.error(f"Failed to read input file: {e}")
    raise


def get_sentiment_score_combined(text, summary):
    combined = " ".join(filter(None, [str(summary or ""), str(text or "")])).strip()
    if not combined:
        return None
    try:
        return float(sia.polarity_scores(combined)['compound'])
    except:
        return None


def classify_sentiment(score):
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


sentiment_udf = udf(get_sentiment_score_combined, FloatType())
classify_udf = udf(classify_sentiment, StringType())

logger.info("Performing sentiment analysis...")
transform_start = time.time()

try:
    df_with_sentiment = df \
        .withColumn("sentiment_score", sentiment_udf(col("Text"), col("Summary"))) \
        .withColumn("satisfaction_level", classify_udf(col("sentiment_score"))) \
        .withColumn("has_text_review", when(
        (col("Text").isNotNull() & (col("Text") != "")) |
        (col("Summary").isNotNull() & (col("Summary") != "")),
        lit(1)).otherwise(lit(0))) \
        .withColumn("has_score_value", when(col("Score").isNotNull(), lit(1)).otherwise(lit(0)))

    df_with_sentiment.cache()
    sentiment_count = df_with_sentiment.count()
    transform_time = time.time() - transform_start

    null_sentiments = df_with_sentiment.filter(col("sentiment_score").isNull()).count()
    if null_sentiments > row_count * 0.1:
        logger.warning(
            f"{null_sentiments:,} rows ({null_sentiments / row_count * 100:.1f}%) have null sentiment scores")

    logger.info(f"Sentiment analysis complete ({transform_time:.1f}s, {row_count / transform_time:.0f} rows/s)")

except Exception as e:
    logger.error(f"Sentiment analysis failed: {e}")
    raise

output_path_reviews = "/opt/spark-output/reviews_with_sentiment.csv"
logger.info(f"Saving detailed results to {output_path_reviews}")
write_start = time.time()

try:
    df_with_sentiment.select(
        "Id", "ProductId", "UserId", "ProfileName", "HelpfulnessNumerator",
        "HelpfulnessDenominator", "Score", "Time", "Summary", "Text",
        "sentiment_score", "satisfaction_level"
    ).coalesce(1).write.mode("overwrite").csv(output_path_reviews, header=True)

    write_time = time.time() - write_start
    logger.info(f"Detailed results saved successfully ({write_time:.1f}s)")

except Exception as e:
    logger.error(f"Failed to save detailed results: {e}")
    raise

logger.info("Creating product-level aggregation...")
agg_start = time.time()

try:
    satisfaction_levels = ["very_satisfied", "somewhat_satisfied", "neutral",
                           "somewhat_dissatisfied", "very_dissatisfied"]
    scores = [1, 2, 3, 4, 5]

    agg_exprs = {
        "total_number_of_reviews": count("*"),
        "rows_without_text": count(when(col("has_text_review") == 0, 1)),
        "rows_without_score": count(when(col("has_score_value") == 0, 1)),
        "rows_with_text": count(when(col("has_text_review") == 1, 1)),
        "rows_with_score": count(when(col("has_score_value") == 1, 1)),
        "avg_sentiment_score": avg(when(col("sentiment_score").isNotNull(), col("sentiment_score"))),
        "avg_review_score": avg(when(col("Score").isNotNull(), col("Score"))),
    }

    for level in satisfaction_levels:
        title_case = level.replace("_", " ").title().replace(" ", " ")
        agg_exprs[f"{level}_count"] = count(
            when((col("satisfaction_level") == title_case) & (col("has_text_review") == 1), 1)
        )

    for score in scores:
        agg_exprs[f"score_{score}_count"] = count(
            when((col("Score") == score) & (col("has_score_value") == 1), 1)
        )

    product_agg = df_with_sentiment.groupBy("ProductId").agg(*[
        expr.alias(name) for name, expr in agg_exprs.items()
    ])

    product_agg.cache()
    unique_products = product_agg.count()

    logger.info(
        f"Aggregated {unique_products:,} unique products (avg {row_count / unique_products:.1f} reviews/product)")

except Exception as e:
    logger.error(f"Product aggregation failed: {e}")
    raise

logger.info("Calculating percentage metrics...")

try:
    percentage_configs = [
                             ("pct_without_text_review", "rows_without_text", "total_number_of_reviews", None, 2),
                             ("pct_without_score_review", "rows_without_score", "total_number_of_reviews", None, 2),
                         ] + [
                             (f"{level}_pct", f"{level}_count", "rows_with_text", "rows_with_text", 2)
                             for level in satisfaction_levels
                         ] + [
                             (f"score_{score}_pct", f"score_{score}_count", "rows_with_score", "rows_with_score", 2)
                             for score in scores
                         ]

    product_summary = product_agg
    for new_col, count_col, denom_col, cond_col, decimals in percentage_configs:
        calc = spark_round((col(count_col) / col(denom_col)) * 100, decimals)
        product_summary = product_summary.withColumn(
            new_col,
            when(col(cond_col) > 0, calc).otherwise(lit(None)) if cond_col else calc
        )

    product_summary_final = product_summary \
        .withColumn("avg_sentiment_score", spark_round(col("avg_sentiment_score"), 4)) \
        .withColumn("avg_review_score", spark_round(col("avg_review_score"), 2)) \
        .select(
        col("ProductId").alias("product_id"),
        "total_number_of_reviews", "avg_sentiment_score", "avg_review_score",
        "pct_without_text_review", *[f"{l}_pct" for l in satisfaction_levels],
        "pct_without_score_review", *[f"score_{s}_pct" for s in [5, 4, 3, 2, 1]]
    )

    agg_time = time.time() - agg_start
    logger.info(f"Product summary prepared ({agg_time:.1f}s)")

except Exception as e:
    logger.error(f"Percentage calculation failed: {e}")
    raise

output_path_summary = "/opt/spark-output/product_sentiment_summary.csv"
logger.info(f"Saving product summary to {output_path_summary}")
summary_write_start = time.time()

try:
    product_summary_final.coalesce(1).write.mode("overwrite").csv(
        output_path_summary, header=True
    )

    summary_write_time = time.time() - summary_write_start
    logger.info(f"Product summary saved successfully ({summary_write_time:.1f}s)")

except Exception as e:
    logger.error(f"Failed to save product summary: {e}")
    raise

total_time = time.time() - start_time
logger.info("=" * 60)
logger.info("Pipeline completed successfully")
logger.info(f"Execution time: {total_time:.1f}s ({total_time / 60:.1f}m)")
logger.info(f"Total reviews: {row_count:,} | Unique products: {unique_products:,}")
logger.info(f"Processing rate: {row_count / total_time:.0f} rows/second")

spark.stop()
logger.info("Spark session stopped")