import argparse
import logging
import os
from datetime import datetime

import pyspark
from delta import configure_spark_with_delta_pip

from utils.config import (BRONZE_PATH, BRONZE_TABLES, DATA_SOURCE_PATH,
                          DPD_VALUE, END_DATE, GOLD_PATH, GOLD_TABLES,
                          MOB_VALUE, SILVER_PATH, SILVER_TABLES, START_DATE)
from utils.data_processing_bronze import process_bronze_table
from utils.data_processing_gold import (process_features_gold_table,
                                        process_labels_gold_table)
from utils.data_processing_silver import process_silver_table

parser = argparse.ArgumentParser()
parser.add_argument(
    "--log-level",
    default="INFO",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    help="Set the logging level (default: INFO)",
)
args = parser.parse_args()

logging.basicConfig(
    level=getattr(logging, args.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Initialise SparkSession with Delta Lake extensions
builder = (
    pyspark.sql.SparkSession.builder.appName("dev")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
    if not os.path.exists(path):
        os.makedirs(path)


def generate_first_of_month_dates(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # List to store the first of month dates
    first_of_month_dates = []

    # Start from the first of the month of the start_date
    current_date = datetime(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Append the date in yyyy-mm-dd format
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))

        # Move to the first of the next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)

    return first_of_month_dates


dates_str_lst = generate_first_of_month_dates(START_DATE, END_DATE)
logger.info(f"Pipeline started | date range: {START_DATE} to {END_DATE} ({len(dates_str_lst)} months)")

# bronze backfill
logger.info("Starting bronze backfill")
for date_str in dates_str_lst:
    for table_name, table_config in BRONZE_TABLES.items():
        logger.debug(f"Bronze: processing {table_name} for {date_str}")
        process_bronze_table(table_name, table_config, DATA_SOURCE_PATH, BRONZE_PATH, spark, date_str)
logger.info("Bronze backfill complete")

# silver backfill
logger.info("Starting silver backfill")
for date_str in dates_str_lst:
    for table_name, table_config in SILVER_TABLES.items():
        logger.debug(f"Silver: processing {table_name} for {date_str}")
        process_silver_table(table_name, table_config, BRONZE_PATH, SILVER_PATH, spark, date_str)
logger.info("Silver backfill complete")

# gold backfill
logger.info("Starting gold backfill")
for date_str in dates_str_lst:
    logger.debug(f"Gold: processing label store for {date_str}")
    process_labels_gold_table("lms_loan_daily", GOLD_TABLES["label_store"], SILVER_PATH, GOLD_PATH, MOB_VALUE, DPD_VALUE, spark, date_str)
    logger.debug(f"Gold: processing feature store for {date_str}")
    process_features_gold_table(GOLD_TABLES["feature_store"], SILVER_PATH, GOLD_PATH, spark, date_str)
logger.info("Gold backfill complete")

logger.info("Pipeline finished")
