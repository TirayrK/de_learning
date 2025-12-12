"""
Data Validation Module
Validates input CSV and output Parquet files
"""

import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates input and output data"""

    def __init__(self, staging_bucket, output_bucket, data_prefix, processed_prefix):
        self.staging_bucket = staging_bucket
        self.output_bucket = output_bucket
        self.data_prefix = data_prefix
        self.processed_prefix = processed_prefix
        self._gcs_hook = None

    @property
    def gcs_hook(self):
        if self._gcs_hook is None:
            self._gcs_hook = GCSHook()
        return self._gcs_hook

    def get_unprocessed_files(self):
        """Get all CSV files from unprocessed folder"""
        all_files = self.gcs_hook.list(bucket_name=self.staging_bucket, prefix=self.data_prefix)
        csv_files = [f for f in all_files if f.endswith('.csv')]

        if not csv_files:
            raise FileNotFoundError(f"No CSV files in gs://{self.staging_bucket}/{self.data_prefix}")

        return csv_files

    def validate_input_csv(self):
        """Validate input CSV has required columns and correct schema"""
        files = self.get_unprocessed_files()

        required_columns = {
            'Id', 'ProductId', 'UserId', 'ProfileName',
            'HelpfulnessNumerator', 'HelpfulnessDenominator',
            'Score', 'Time', 'Summary', 'Text'
        }

        for input_file in files:
            if not self.gcs_hook.exists(bucket_name=self.staging_bucket, object_name=input_file):
                continue

            content = self.gcs_hook.download(
                bucket_name=self.staging_bucket,
                object_name=input_file,
                num_max_attempts=1
            )
            content = content[:2048].decode('utf-8', errors='ignore')
            lines = content.split('\n')

            if len(lines) < 2:
                raise ValueError(f"File {input_file} is empty or has no data rows")

            header = lines[0].strip()
            columns = [col.strip().strip('"') for col in header.split(',')]

            missing = required_columns - set(columns)
            if missing:
                raise ValueError(f"File {input_file} missing required columns: {missing}")

            extra = set(columns) - required_columns
            if extra:
                logger.info(f"File {input_file} has extra columns (will be ignored): {extra}")

            if len(lines) < 2 or not lines[1].strip():
                raise ValueError(f"File {input_file} has header but no data rows")

            logger.info(f"Validated {input_file}: {len(columns)} columns, all required columns present")

        logger.info(f"All {len(files)} input file(s) validated successfully")
        return True

    def validate_output_parquet(self):
        """Validate output parquet files exist and contain data"""
        reviews_prefix = "reviews_with_sentiment_parquet/"
        summary_prefix = "product_sentiment_summary_parquet/"

        reviews_files = self.gcs_hook.list(bucket_name=self.output_bucket, prefix=reviews_prefix)
        summary_files = self.gcs_hook.list(bucket_name=self.output_bucket, prefix=summary_prefix)

        reviews_files = [f for f in reviews_files if f.endswith('.parquet')]
        summary_files = [f for f in summary_files if f.endswith('.parquet')]

        if not reviews_files:
            raise FileNotFoundError(f"No reviews parquet files found in gs://{self.output_bucket}/{reviews_prefix}")
        if not summary_files:
            raise FileNotFoundError(f"No summary parquet files found in gs://{self.output_bucket}/{summary_prefix}")

        reviews_size = sum(
            self.gcs_hook.get_size(bucket_name=self.output_bucket, object_name=f)
            for f in reviews_files
        )
        summary_size = sum(
            self.gcs_hook.get_size(bucket_name=self.output_bucket, object_name=f)
            for f in summary_files
        )

        if reviews_size == 0:
            raise ValueError("Reviews parquet files are empty (0 bytes)")
        if summary_size == 0:
            raise ValueError("Summary parquet files are empty (0 bytes)")

        logger.info(f"Reviews output: {len(reviews_files)} file(s), {reviews_size:,} bytes")
        logger.info(f"Summary output: {len(summary_files)} file(s), {summary_size:,} bytes")
        logger.info(f"All output files validated successfully")

        return True