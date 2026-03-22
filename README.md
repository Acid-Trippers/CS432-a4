# Autonomous Normalization & CRUD Engine Hybrid Database Framework

_(CS432 - Databases Assignment 2)_

This repository implements an intelligent, hybrid database framework that dynamically ingests, categorizes, normalizes, and routes JSON data across SQL (PostgreSQL) and NoSQL (MongoDB) databases. It completely orchestrates database schema creation, intelligent row/document structures, relational setups, and data mapping—without relying on static predefined schemas.

## Project Overview

The pipeline detects when data is highly recursive/nested and processes it accordingly:

- **SQL (PostgreSQL):** Resolves structured arrays and repeating entities into fully normalized tables, managing Primary and Foreign Keys automatically.
- **MongoDB:** Analyzes unstructured nested structures to intelligently decide if they should be embedded (smaller objects) or extracted into separate collections (frequently updated, large collections) and linked by references.
- **Query/CRUD Operations:** Exposes a unified API-like query runner that uses generated metadata to interact with both databases behind the scenes seamlessly.

## Project Structure

```text
CS432-a2/
│
├── data/              # Stores the temporary processing states (Buffer, Checkpoints, Metadata)
├── docs/              # Supplemental Architecture and Docker Guides
├── external/          # Contains mock external APIs (app.py) where data is fetched from
├── src/               # The Core Pipeline implementation
│   ├── phase_1_to_4/  # Data Profiling, Cleaning, Intelligent Classification & Metadata Gen
│   ├── phase_5/       # Storage Engine Execution (SQL generation, MongoDB collections)
│   ├── phase_6/       # The Database CRUD User Interface & Query Engine Execution
│   └── config.py      # Environment Constants
│
├── docker-compose.yml # Docker Services Definition (PostgreSQL, MongoDB)
├── main.py            # Main entrypoint to run the Database Pipeline Operations
├── requirements.txt   # Python Dependencies
└── starter.py         # Standalone utility to orchestrate Docker containers logic
```

## The Pipeline Architecture

The system completes its work in six distinct phases:

1. **Phase 1-2 (Ingestion & Schema Definition):** Fetches deeply nested JSON inputs from the mock API endpoint and cleans the datasets, stripping out missing or invalid structural issues.
2. **Phase 3 (Profiling & Analysis):** Recursively analyzes payload nodes tracking the density (datatypes vs lengths) and the complexity factor of each distinct array, string, and object entity.
3. **Phase 4 (Classification & Metadata):** Routes top-layer columns directly. Depending upon the structure, deeply nested complex entities are divided into relational tables (SQL) or documents/collections (MongoDB). It outputs an instruction manual (`metadata.json`) routing how properties will map locally. Unresolved anomalies wait in a pipeline `buffer`.
4. **Phase 5 (Database Engineering/Storage):** The respective SQL and MongoDB Engines deploy. Utilizing the metadata routing, tables are established holding strict normal forms and keys schemas. MongoDB stores complex fields via referential lookups or sub-document structures depending upon footprint rules.
5. **Phase 6 (Query Engine):** Handles user-supplied CRUD instructions (`query.json`) and translates properties using the Metadata map into raw parameterized queries sent asynchronously to Postgres and Mongo as required. Results are merged into a final coherent JSON response.

---

## How To Use The Repository

### 1. Clone the Repository

First, clone the repository to your local machine and navigate into the directory:

```powershell
git clone https://github.com/Acid-Trippers/CS432-a2
cd CS432-a2
```

### 2. Requirements Setup

Ensure you have the following installed on your machine:

- **Python 3.8+**
- **Docker Desktop**
- **Pip** dependencies:
  ```powershell
  pip install -r requirements.txt
  ```

### 3. Booting Up the Environment

This project includes an environment bootstrapper. Start the PostgreSQL, MongoDB instances, and network bridge using the helper script:

```powershell
python starter.py start
```

_(When finished ie after ingestion and querying, before ending the session don't forget to run `python starter.py end` to shut down the containers cleanly)._

### 4. Running The Initialisation Pipeline

To orchestrate a clean environment setup, perform a DB wipe, and spin up an entirely fresh pipeline with the primary API dataset (defaults to 1000 items, or you can supply your count):

```powershell
python main.py initialise 500
```

This triggers **Phase 1 through Phase 5**, creating SQL table schema relationships, MongoDB clusters, and loading that batch automatically!

### 5. Fetching & Incremental Sync (Batched Processing)

Once initialized, if you want the Database intelligence layer to ingest an additional X records and automatically adjust normalizations dynamically:

```powershell
python main.py fetch 200
```

_(The system keeps track of progress checkpoints and correctly handles appends or adaptations via the evolution suite)_.

### 6. Executing CRUD Operations Across Infrastructure

Use the hybrid query engine to read, insert, update, or delete records globally across databases!

```powershell
python main.py query
```

The system will display an interactive CLI UI via `CRUD_json_reader.py` allowing you to draft your data operation seamlessly. It then compiles the internal instruction, matches properties with the generated `metadata.json`, and routes the sub-queries directly to the correct database layer concurrently.

---

## Documentation & Resources

For detailed insights into specific components of the framework, refer to the following guide documents available in the `docs/` folder:

- **[SQL Engine Architecture](docs/SQL_ENGINE_ARCHITECTURE.md)**: Details on the automated normalization techniques, key extraction, and repeating entity algorithms.
- **[Docker Setup & Usage Guide](docs/DOCKER_GUIDE.md)**: A complete technical breakdown for administrating the customized PostgreSQL and MongoDB containers.
- **[SQL Pipeline Guide](docs/sql_pipeline_guide.md)**: Learnings and rules backing the `src/phase_5/sql_pipeline.py`.
- **[Assignment 2 Guidelines](docs/assignment-2-guidelines.md)**: The original curriculum requirements from the IIT Gandhinagar course.

---

_Built incrementally mapping to Assignment Guidelines for Course Project CS 432._
