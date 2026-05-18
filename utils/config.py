import yaml

with open("schema.yaml") as f:
    schema = yaml.safe_load(f)

with open("config.yaml") as f:
    config = yaml.safe_load(f)

DATA_SOURCE_PATH = schema["source"]["path"]
BRONZE_PATH = schema["bronze"]["path"]
BRONZE_TABLES = schema["bronze"]["tables"]
SILVER_PATH = schema["silver"]["path"]
SILVER_TABLES = schema["silver"]["tables"]
GOLD_PATH = schema["gold"]["path"]
GOLD_TABLES = schema["gold"]["tables"]

START_DATE = config["backfill"]["start_date"]
END_DATE = config["backfill"]["end_date"]
DPD_VALUE = config["label"]["dpd"]
MOB_VALUE = config["label"]["mob"]
