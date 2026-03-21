# Docker Guide: CS432-a2 Data Pipeline

This document explains the Docker configuration for the CS432-a2 project, including the multi-container environment, service descriptions, and how to run the data pipeline within Docker.

## 🏗️ Architecture Overview

The project uses `docker-compose` to orchestrate a complete environment for data ingestion, processing, and storage.

```mermaid
graph TD
    Pipeline[Pipeline Container] --> |Fetches Data| API[API Container]
    Pipeline --> |Stores Relational Data| Postgres[Postgres Container]
    Pipeline --> |Stores Document Data| Mongo[Mongo Container]
    API --> |Simulates Stream| Pipeline
```

### Services

1.  **`postgres`**: A PostgreSQL 16 relational database.
    *   **Port**: `5432`
    *   **User**: `admin` / **Password**: `secret`
    *   **DB Name**: `cs432_db`
2.  **`mongo`**: A MongoDB 7 document database.
    *   **Port**: `27017`
    *   **User**: `admin` / **Password**: `secret`
3.  **`api`**: A FastAPI-based external data source simulator.
    *   **Port**: `8000`
    *   **Purpose**: Provides streaming JSON data for ingestion.
4.  **`pipeline`**: The main processing service.
    *   **Purpose**: Runs the cleaning, profiling, and SQL normalization logic.
    *   **Base Image**: `python:3.11-slim`

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed.
- [Docker Compose](https://docs.docker.com/compose/install/) installed.

### Commands

**1. Build and Start All Services**
```bash
docker-compose up --build -d
```
*The `-d` flag runs them in the background.*

**2. Verify Containers are Running**
```bash
docker-compose ps
```

**3. View Logs**
```bash
docker-compose logs -f pipeline
```

**4. Stop All Services**
```bash
docker-compose down
```

---

## 🛠️ Running the Pipeline

The `pipeline` container is configured to stay alive (using `tail -f /dev/null`) so you can execute commands inside it manually.

### Initialise the System
To run the full initialization (schema definition, initial fetch, cleaning, analysis, and SQL insertion):
```bash
docker-compose exec pipeline python main.py initialise 1000
```

### Fetch More Data
To fetch additional records and append them to the existing pipeline:
```bash
docker-compose exec pipeline python main.py fetch 100
```

---

## 📂 Persistent Storage

The project uses a mix of Docker volumes and local bind mounts to ensure data persists across container restarts:

- **Local `data/` directory**: Mounted to `/app/data` in both the `api` and `pipeline` containers. This is where your JSON checkpoints and SQLite/metadata files are stored.
- **`postgres_data` volume**: Persistent storage for the PostgreSQL database.
- **`mongo_data` volume**: Persistent storage for the MongoDB database.

---

## 🔧 Environment Variables

The `pipeline` service is configured with several environment variables to facilitate connection to other services:

| Variable | Value |
| :--- | :--- |
| `POSTGRES_URI` | `postgresql://admin:secret@postgres:5432/cs432_db` |
| `MONGO_URI` | `mongodb://admin:secret@mongo:27017/` |
| `API_HOST` | `http://api:8000` |

> [!NOTE]
> Currently, `main.py` starts its own local API process for simplicity. Future updates will transition to using the `API_HOST` variable to communicate with the standalone `api` container.

---

## ❓ Troubleshooting

### Port Conflicts
If you have a local PostgreSQL or MongoDB instance running, you might see an error like:
`Bind for 0.0.0.0:5432 failed: port is already allocated`.

**Fix:** Either stop your local services or change the host port mapping in `docker-compose.yml`.

### Permission Issues
The `data/` directory is shared with the container. If the container cannot write to it, you may need to check the permissions of your local `data/` folder.
