import os

# Project root directory
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory
DATA_DIR = os.path.join(ROOT_DIR, "data")

# JSON and Data File Paths
INITIAL_SCHEMA_FILE = os.path.join(DATA_DIR, "initial_schema.json")
RECEIVED_DATA_FILE = os.path.join(DATA_DIR, "received_data.json")
CLEANED_DATA_FILE = os.path.join(DATA_DIR, "cleaned_data.json")
BUFFER_FILE = os.path.join(DATA_DIR, "buffer.json")
ANALYZED_SCHEMA_FILE = os.path.join(DATA_DIR, "analyzed_schema.json")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")  # Unified metadata file for all stages
SQL_DATA_FILE = os.path.join(DATA_DIR, "sql_data.json")
QUERY_FILE = os.path.join(DATA_DIR, "query.json")

# Database configuration
DATABASE_PATH = os.path.join(DATA_DIR, "engine.db")
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# Other data files
COUNTER_FILE = os.path.join(DATA_DIR, "counter.txt")
# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
