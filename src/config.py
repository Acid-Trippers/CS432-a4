import os

# Project root directory
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory
DATA_DIR = os.path.join(ROOT_DIR, "data")

# JSON File Paths
RECEIVED_DATA_FILE = os.path.join(DATA_DIR, "received_data.json")
CLEANED_DATA_FILE = os.path.join(DATA_DIR, "cleaned_data.json")
NORMALIZED_DATA_FILE = os.path.join(DATA_DIR, "normalized_data.json")
METADATA_MANAGER_FILE = os.path.join(DATA_DIR, "metadata_manager.json")
INITIAL_SCHEMA_FILE = os.path.join(DATA_DIR, "initial_schema.json")
ANALYZED_SCHEMA_FILE = os.path.join(DATA_DIR, "analyzed_schema.json")
METADATA_FILE = os.path.join(DATA_DIR, "metadata.json")

# Other data files
COUNTER_FILE = os.path.join(DATA_DIR, "counter.txt")
BUFFER_FILE = os.path.join(DATA_DIR, "buffer.txt")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
