import yaml

with open("config.yaml") as _f:
    _config = yaml.safe_load(_f)

DATA_SOURCE_PATH = _config["source"]["path"]
BRONZE_PATH = _config["bronze"]["path"]
BRONZE_TABLES = _config["bronze"]["tables"]
