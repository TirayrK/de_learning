from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, when, avg, round as spark_round, lit
from pyspark.sql.types import StringType, FloatType
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("=" * 80)
logger.info("Starting Amazon Reviews Sentiment Analysis Pipeline")
logger.info("=" * 80)

# Download NLTK data if needed
logger.info("Checking NLTK vader_lexicon data...")
try:
    nltk.data.find('vader_lexicon')
    logger.info("✓ vader_lexicon found")
except LookupError:
    logger.info("Downloading vader_lexicon...")
    nltk.download('vader_lexicon')
    logger.info("✓ vader_lexicon downloaded")

# Initialize Spark Session
logger.info("Initializing Spark Session...")
start_time = time.time()

spark = SparkSession.builder \
    .appName("Amazon Reviews Sentiment Analysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark_init_time = time.time() - start_time
logger.info(f"✓ Spark Session initialized in {spark_init_time:.2f} seconds")
logger.info(f"Spark Version: {spark.version}")
logger.info(f"Master: {spark.sparkContext.master}")

# Initialize NLTK Sentiment Analyzer
logger.info("Initializing NLTK Sentiment Analyzer...")
sia = SentimentIntensityAnalyzer()
logger.info("✓ Sentiment Analyzer ready")

# Read input CSV from GCS
input_path = "gs://sada-tirayr-sentiment-staging/data/Reviews.csv"
logger.info(f"Reading input data from: {input_path}")
read_start = time.time()

df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

# Cache and count for logging
df.cache()
row_count = df.count()
read_time = time.time() - read_start

logger.info(f"✓ Data loaded successfully")
logger.info(f"  - Total rows: {row_count:,}")
logger.info(f"  - Columns: {len(df.columns)}")
logger.info(f"  - Column names: {', '.join(df.columns)}")
logger.info(f"  - Read time: {read_time:.2f} seconds")

# Show sample data
logger.info("Sample data (first 3 rows):")
df.show(3, truncate=50)

# UDF for sentiment score from combined Text + Summary
def get_sentiment_score_combined(text, summary):
    combined = " ".join(filter(None, [str(summary or ""), str(text or "")])).strip()
    if not combined:
        return None
    try:
        return float(sia.polarity_scores(combined)['compound'])
    except:
        return None

# UDF for satisfaction level classification
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

# Register UDFs
logger.info("Registering UDFs...")
sentiment_udf = udf(get_sentiment_score_combined, FloatType())
classify_udf = udf(classify_sentiment, StringType())
logger.info("✓ UDFs registered")

# Apply sentiment analysis
logger.info("Applying sentiment analysis transformations...")
transform_start = time.time()

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

logger.info(f"✓ Sentiment analysis complete")
logger.info(f"  - Rows processed: {sentiment_count:,}")
logger.info(f"  - Processing time: {transform_time:.2f} seconds")
logger.info(f"  - Average time per row: {(transform_time/sentiment_count)*1000:.2f} ms")

# Show sample with sentiment
logger.info("Sample data with sentiment scores:")
df_with_sentiment.select(
    "ProductId", "Score", "sentiment_score", "satisfaction_level"
).show(5, truncate=False)

# Save detailed results to GCS as PARQUET
output_path_reviews = "gs://sada-tirayr-sentiment-output/reviews_with_sentiment_parquet/"
logger.info(f"Writing detailed reviews to: {output_path_reviews}")
write_start = time.time()

df_with_sentiment.select(
    "Id", "ProductId", "UserId", "ProfileName", "HelpfulnessNumerator",
    "HelpfulnessDenominator", "Score", "Time", "Summary", "Text",
    "sentiment_score", "satisfaction_level"
).write.mode("overwrite").parquet(output_path_reviews)

write_time = time.time() - write_start
logger.info(f"✓ Detailed reviews written successfully")
logger.info(f"  - Write time: {write_time:.2f} seconds")

# Aggregate by product
logger.info("Starting product-level aggregation...")
agg_start = time.time()

satisfaction_levels = ["very_satisfied", "somewhat_satisfied", "neutral",
                      "somewhat_dissatisfied", "very_dissatisfied"]
scores = [1, 2, 3, 4, 5]

# Build aggregation expressions
agg_exprs = {
    "total_number_of_reviews": count("*"),
    "rows_without_text": count(when(col("has_text_review") == 0, 1)),
    "rows_without_score": count(when(col("has_score_value") == 0, 1)),
    "rows_with_text": count(when(col("has_text_review") == 1, 1)),
    "rows_with_score": count(when(col("has_score_value") == 1, 1)),
    "avg_sentiment_score": avg(when(col("sentiment_score").isNotNull(), col("sentiment_score"))),
    "avg_review_score": avg(when(col("Score").isNotNull(), col("Score"))),
}

# Add satisfaction counts
for level in satisfaction_levels:
    title_case = level.replace("_", " ").title().replace(" ", " ")
    agg_exprs[f"{level}_count"] = count(
        when((col("satisfaction_level") == title_case) & (col("has_text_review") == 1), 1)
    )

# Add score counts
for score in scores:
    agg_exprs[f"score_{score}_count"] = count(
        when((col("Score") == score) & (col("has_score_value") == 1), 1)
    )

logger.info(f"Grouping by ProductId with {len(agg_exprs)} aggregations...")
product_agg = df_with_sentiment.groupBy("ProductId").agg(*[
    expr.alias(name) for name, expr in agg_exprs.items()
])

product_agg.cache()
unique_products = product_agg.count()
logger.info(f"✓ Aggregation complete")
logger.info(f"  - Unique products: {unique_products:,}")

# Calculate percentages
logger.info("Calculating percentage metrics...")
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

logger.info("✓ Percentage calculations complete")

# Round and select final columns
logger.info("Finalizing product summary schema...")
product_summary_final = product_summary \
    .withColumn("avg_sentiment_score", spark_round(col("avg_sentiment_score"), 4)) \
    .withColumn("avg_review_score", spark_round(col("avg_review_score"), 2)) \
    .select(
        col("ProductId").alias("product_id"),
        "total_number_of_reviews", "avg_sentiment_score", "avg_review_score",
        "pct_without_text_review", *[f"{l}_pct" for l in satisfaction_levels],
        "pct_without_score_review", *[f"score_{s}_pct" for s in [5,4,3,2,1]]
    )

agg_time = time.time() - agg_start
logger.info(f"✓ Product summary prepared")
logger.info(f"  - Aggregation time: {agg_time:.2f} seconds")

# Show sample product summary
logger.info("Sample product summary (first 5 products):")
product_summary_final.show(5, truncate=False)

# Save product summary to GCS as PARQUET
output_path_summary = "gs://sada-tirayr-sentiment-output/product_sentiment_summary_parquet/"
logger.info(f"Writing product summary to: {output_path_summary}")
summary_write_start = time.time()

product_summary_final.write.mode("overwrite").parquet(output_path_summary)

summary_write_time = time.time() - summary_write_start
logger.info(f"✓ Product summary written successfully")
logger.info(f"  - Write time: {summary_write_time:.2f} seconds")

# Final statistics
total_time = time.time() - start_time
logger.info("=" * 80)
logger.info("Pipeline Execution Summary")
logger.info("=" * 80)
logger.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
logger.info(f"")
logger.info(f"Phase Breakdown:")
logger.info(f"  1. Spark initialization:    {spark_init_time:.2f}s ({spark_init_time/total_time*100:.1f}%)")
logger.info(f"  2. Data reading:            {read_time:.2f}s ({read_time/total_time*100:.1f}%)")
logger.info(f"  3. Sentiment analysis:      {transform_time:.2f}s ({transform_time/total_time*100:.1f}%)")
logger.info(f"  4. Write detailed reviews:  {write_time:.2f}s ({write_time/total_time*100:.1f}%)")
logger.info(f"  5. Product aggregation:     {agg_time:.2f}s ({agg_time/total_time*100:.1f}%)")
logger.info(f"  6. Write product summary:   {summary_write_time:.2f}s ({summary_write_time/total_time*100:.1f}%)")
logger.info(f"")
logger.info(f"Data Statistics:")
logger.info(f"  - Total reviews processed:  {row_count:,}")
logger.info(f"  - Unique products:          {unique_products:,}")
logger.info(f"  - Average reviews/product:  {row_count/unique_products:.1f}")
logger.info(f"  - Processing rate:          {row_count/total_time:.0f} rows/second")
logger.info("=" * 80)
logger.info("✓ Pipeline completed successfully!")
logger.info("=" * 80)

spark.stop()
logger.info("Spark session stopped")
