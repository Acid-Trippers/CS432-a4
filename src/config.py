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
MONGO_DATA_FILE = os.path.join(DATA_DIR, "mongo_data.json")
QUERY_FILE = os.path.join(DATA_DIR, "query.json")
QUERY_OUTPUT_FILE = os.path.join(DATA_DIR, "query_output.json")
CHECKPOINT_FILE = os.path.join(DATA_DIR, "checkpoint.json")

# Database configuration
DATABASE_URL = os.environ.get("POSTGRES_URI", "postgresql://admin:secret@localhost:5432/cs432_db")

# MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:secret@localhost:27017/")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "cs432_db")

# External API
API_HOST = os.environ.get("API_HOST", "http://127.0.0.1:8000")

# Other data files
COUNTER_FILE = os.path.join(DATA_DIR, "counter.txt")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)