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

# bronze backfill
for date_str in dates_str_lst:
    for table_name, table_config in BRONZE_TABLES.items():
        process_bronze_table(table_name, table_config, DATA_SOURCE_PATH, BRONZE_PATH, spark, date_str)

# silver backfill
for date_str in dates_str_lst:
    for table_name, table_config in SILVER_TABLES.items():
        process_silver_table(table_name, table_config, BRONZE_PATH, SILVER_PATH, spark, date_str)

# gold backfill
for date_str in dates_str_lst:
    process_labels_gold_table("lms_loan_daily", GOLD_TABLES["label_store"], SILVER_PATH, GOLD_PATH, MOB_VALUE, DPD_VALUE, spark, date_str)
    process_features_gold_table(GOLD_TABLES["feature_store"], SILVER_PATH, GOLD_PATH, spark, date_str)
