# project_config.py

# --- Run Mode ---
DEFAULT_COMMAND = "initialise"   # "initialise" or "fetch"

# --- Record Counts ---
INITIALISE_COUNT = 1000
FETCH_COUNT = 100

# --- API Server ---
API_HOST = "127.0.0.1"
API_PORT = 8000
API_STARTUP_TIMEOUT = 15        # seconds to wait for API to be ready

# --- Docker ---
DOCKER_COMPOSE_FILE = "docker-compose.yml"
MONGO_CONTAINER = "mongo"
POSTGRES_CONTAINER = "postgres"
DOCKER_STARTUP_TIMEOUT = 20     # seconds to wait for containers to be ready

# --- MongoDB ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "hybrid_db"

# --- PostgreSQL ---
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB_NAME = "hybrid_db"
PG_USER = "admin"
PG_PASSWORD = "admin"
