# SQL Engine Architecture & Concepts

## Table of Contents
1. [Core Concepts](#core-concepts)
2. [Design Philosophy](#design-philosophy)
3. [System Overview](#system-overview)
4. [File-by-File Breakdown](#file-by-file-breakdown)
5. [Data Flow Diagram](#data-flow-diagram)
6. [Key Functions & Their Impact](#key-functions--their-impact)
7. [Example Walkthrough](#example-walkthrough)

---

## Core Concepts

### 1. **Data Normalization**
Converting nested/complex JSON data into a relational (flat) SQL structure while maintaining relationships.

**Problem:** Raw nested JSON like:
```json
{
  "name": "John",
  "email": "john@example.com",
  "metadata": {
    "sensor_data": {
      "temperature": 25.5,
      "readings": [4, 2, 6]
    }
  },
  "tags": ["important", "reviewed"]
}
```

**Solution:** Split into multiple related tables:
- `main_records` (root fields)
- `main_records_metadata` (nested object)
- `main_records_tags` (array of primitives)
- `main_records_readings` (array from deep nesting)

### 2. **Schema-Driven Architecture**
Ground truth comes from `initial_schema.json` (user-defined). Metadata enrichment from analyzer identifies:
- Which fields are nested objects
- Which fields are arrays
- Which fields are primitive/root-level
- Array content type (objects vs primitives)

### 3. **Table Hierarchy**
```
main_records (root table)
    ├── main_records_metadata (nested object from "metadata" field)
    │   └── main_records_metadata_sensor_data (deeply nested object)
    │       └── main_records_metadata_sensor_data_readings (array of primitives)
    └── main_records_tags (array of primitives)
```

Every child table has a **Foreign Key** back to its parent for referential integrity.

### 4. **Three-Layer Processing**

```
Layer 1: SCHEMA DEFINITION (sql_schema_definer.py)
         Input: initial_schema.json + metadata_manager.json
         Output: SQLAlchemy models + database tables

Layer 2: DATA NORMALIZATION & ENGINE (sql_engine.py)
         Input: Nested JSON records + SQLAlchemy models
         Output: Normalized records inserted into SQL tables

Layer 3: PIPELINE ORCHESTRATION (sql_pipeline.py)
         Input: sql_data.json (routed data from classification)
         Output: Fully populated database with statistics
```

---

## Design Philosophy

### Why This Approach?

1. **Decoupling Schema from Data**
   - Schema definition happens once (at initialization)
   - Data can be ingested repeatedly without schema changes
   - Flexible for schema evolution

2. **Flexibility in Representation**
   - Root-level fields → single table columns (fast access)
   - Nested objects → separate tables (organized, normalized)
   - Arrays of primitives → junction tables (maintains order, type)
   - Arrays of objects → flexible JSON storage or separate tables

3. **Referential Integrity**
   - All relationships defined via Primary/Foreign Keys
   - Database enforces data consistency
   - Cascading deletes clean up orphaned nested records

4. **Metadata-Driven**
   - No hardcoding of field structures
   - Automatically detects nesting and array patterns
   - Can handle heterogeneous data (different records have different schemas)

5. **Type Safety**
   - SQLAlchemy enforces column types
   - Type mapping from Python schema to SQL types
   - Handles NULL values, uniqueness constraints

---

## System Overview

### Architecture Diagram

```
INPUT DATA (nested JSON)
        ↓
[sql_schema_definer.py] ← initial_schema.json, metadata_manager.json
        ↓
    SQLAlchemy Models (dynamically created)
        ↓
    Database Tables (SQLite/MySQL)
        ↓
[sql_engine.py] 
        ↓ (insert, update, query, delete)
    SQL Database
        ↓
[sql_pipeline.py]
        ↓
    Final Results + Statistics
```

### Key Files

| File | Purpose | Input | Output |
|------|---------|-------|--------|
| **sql_schema_definer.py** | Schema definition & table creation | `initial_schema.json`, `metadata_manager.json` | SQLAlchemy models, database tables |
| **sql_engine.py** | Data operations (insert, query, normalize) | Nested JSON records, SQLAlchemy models | Normalized data in SQL tables |
| **sql_pipeline.py** | Orchestration & reporting | `sql_data.json` | Database populated with statistics |

---

## File-by-File Breakdown

### FILE 1: sql_schema_definer.py
**Purpose:** Transform metadata into SQL schema

#### Classes & Functions

##### **class FieldType (Enum)**
Defines all possible SQL column types:
```python
STRING, INT, FLOAT, BOOL, DATETIME, JSON, ARRAY, OBJECT
```
**Impact:** Standardizes type handling across schema definitions

---

##### **class SQLField (Dataclass)**
Represents a single SQL column:
```python
name: str              # Column name
sql_type: Any         # SQLAlchemy type (String, Integer, etc.)
nullable: bool        # Can NULL values exist?
unique: bool          # Unique constraint?
primary_key: bool     # Is this a PK?
foreign_key: str      # FK reference ("table.column")?
```
**Impact:** Atomic unit of schema definition; enables constraint enforcement

---

##### **class SQLTable (Dataclass)**
Blueprint for an entire SQL table:
```python
table_name: str                    # "main_records"
fields: List[SQLField]            # All columns
primary_keys: List[str]           # PK column names
foreign_keys: Dict[str, str]      # {column: "ref_table.ref_col"}
parent_table: Optional[str]       # If this is a child, who's the parent?
parent_field: Optional[str]       # For junction tables
```
**Impact:** Complete schema definition before database creation

---

##### **class SchemaAnalyzer**
Analyzes metadata to determine table structures.

**Key Methods:**

###### `load_schemas()`
- **What it does:** Loads three JSON files
  - `initial_schema.json` → field names and basic types
  - `metadata_manager.json` → is_nested, is_array flags
  - `field_metadata.json` → advanced field properties
- **Why:** Reads ground truth from user schema + analysis results
- **Impact:** Populates analyzer's internal state for processing

```python
self.initial_schema      # {"email": "string", "age": "int", ...}
self.metadata_manager    # {"email": {...}, "tags": {"is_array": True}, ...}
self.field_metadata      # {"email": {...}, ...}
```

---

###### `get_root_fields()`
- **What it does:** Filters fields that are NOT nested/arrays
- **Logic:** 
  ```
  For each field in initial_schema:
      If field is NOT marked as nested AND NOT marked as array:
          Include in root_fields
  ```
- **Example:**
  ```
  Input: {name, email, tags (array), metadata (object)}
  Output: {name, email}  ← Only root-level
  ```
- **Impact:** Determines which fields go in `main_records` table

---

###### `get_nested_objects()`
- **What it does:** Finds all object fields (NOT arrays)
- **Example:**
  ```
  Input: tags (array), metadata (object), sensor_data (object)
  Output: {metadata, sensor_data}  ← Only objects, not arrays
  ```
- **Impact:** Creates separate tables for nested structures

---

###### `get_arrays()`
- **What it does:** Finds all array fields
- **Example:**
  ```
  Input: tags (array of strings), comments (array of objects)
  Output: {tags, comments}  ← Only arrays
  ```
- **Impact:** Creates junction tables for arrays

---

###### `build_table_hierarchy()`
- **What it does:** Builds parent-child relationships
- **Logic:**
  ```
  main_records (parent)
      ├── main_records_metadata (nested object)
      ├── main_records_tags (array)
      └── main_records_metadata_sensor_data (deeply nested)
  ```
- **Impact:** Defines FK relationships and table organization

---

##### **class SQLSchemaBuilder**
Dynamically creates SQLAlchemy models and actual database tables.

###### `__init__(database_url)`
- **What it does:** Initialize builder with database connection
- **Default:** Uses SQLite at `data/engine.db`
- **Impact:** Sets up engine, metadata, analyzer for later use

---

###### `analyze_and_build()`
- **What it does:** Orchestrates entire schema creation
- **Steps:**
  1. Load schema files
  2. Build table hierarchy
  3. Create SQLAlchemy models dynamically
  4. Create database engine (connection)
  5. Create actual tables in database
- **Impact:** Complete schema setup; database is now ready for data

---

###### `_create_models()`
- **What it does:** Generates Python classes (SQLAlchemy models)
- **Process:**
  1. Creates `MainRecords` model (root level)
  2. Creates models for each nested object
  3. Creates models for each array (junction tables)
- **Example Output:**
  ```python
  class MainRecords(Base):
      __tablename__ = 'main_records'
      id = Column(Integer, primary_key=True)
      name = Column(String(255), nullable=True)
      email = Column(String(255), unique=True)
  
  class MainRecordsMetadata(Base):
      __tablename__ = 'main_records_metadata'
      id = Column(Integer, primary_key=True)
      main_records_id = Column(Integer, ForeignKey('main_records.id'))
      # ... nested fields
  
  class MainRecordsTags(Base):
      __tablename__ = 'main_records_tags'
      id = Column(Integer, primary_key=True)
      main_records_id = Column(Integer, ForeignKey('main_records.id'))
      value = Column(String(255))  # Store array item
      position = Column(Integer)   # Maintain order
  ```
- **Impact:** Models are now usable in SQLAlchemy ORM

---

###### `_create_main_table()`
- **What it does:** Builds root table from root fields
- **Impact:** `main_records` is the central table

---

###### `_create_nested_table(field_name, info)`
- **What it does:** Creates a table for a nested object
- **Approach:** 
  - Table name: `parent_table_fieldname`
  - Adds FK column pointing back to parent
  - No other fields (those would be added recursively for deeper nesting)
- **Impact:** Nested objects are normalized into separate tables

---

###### `_create_array_table(field_name, info)`
- **What it does:** Creates junction table for arrays
- **Two cases:**
  1. **Primitive arrays**: `{id, parent_id, value, value_type, position}`
  2. **Object arrays**: `{id, parent_id, data (JSON), position}`
- **Impact:** Arrays become queryable rows; order preserved via position

---

###### `_create_tables()`
- **What it does:** Actually creates tables in database
- **Command:** `Base.metadata.create_all(self.engine)`
- **Impact:** Database now has schema matching models

---

###### `get_models()` & `get_session()`
- **What they do:** Provide access to models and database connections
- **Impact:** Next layer (sql_engine.py) uses these to insert/query data

---

---

### FILE 2: sql_engine.py
**Purpose:** Handle data operations (insert, query, normalize)

#### Classes & Functions

##### **class DataNormalizer**
Decomposes nested JSON into relational format.

###### `load_metadata()`
- **What it does:** Loads `metadata_manager.json` and `field_metadata.json`
- **Impact:** Normalizer knows which fields are nested/arrays

---

###### `normalize_record(record)`
**The CORE function for data decomposition**

- **Input:** Single nested JSON record
  ```json
  {
    "name": "John",
    "email": "john@example.com",
    "tags": ["important", "urgent"],
    "metadata": {"sensor_data": {"temperature": 25.5}}
  }
  ```

- **Logic:**
  ```
  For each field in record:
      Look up its metadata (is_array? is_nested?)
      
      If ROOT FIELD (not nested, not array):
          Put in root_data
      
      If ARRAY:
          Create table_name = "main_records_fieldname"
          If primitive array: Store as {value, value_type, position}
          If object array: Store as {data (JSON), position}
          Add to nested_data[table_name]
      
      If NESTED OBJECT:
          Create table_name = "main_records_fieldname"
          Store the object dict in nested_data[table_name]
  ```

- **Output:**
  ```python
  root_data = {
      "name": "John",
      "email": "john@example.com"
  }
  
  nested_data = {
      "main_records_tags": [
          {"value": "important", "value_type": "str", "position": 0},
          {"value": "urgent", "value_type": "str", "position": 1}
      ],
      "main_records_metadata": [
          {"sensor_data": {"temperature": 25.5}}
      ]
  }
  ```

- **Why This Design:**
  - Separates concerns (root vs nested)
  - Maintains array order via position
  - Tracks value types for primitives
  - Preserves nested objects for further normalization
  
- **Impact:** Transforms any complexity into flat, insertable records

---

###### `parse_nested_fields(record)`
- **What it does:** Flattens deeply nested objects with dot notation
- **Example:**
  ```python
  Input: {"a": {"b": {"c": 10}}}
  Output: {"a.b.c": 10}
  ```
- **Impact:** Handles arbitrary nesting depth without recursion limits

---

##### **class SQLEngine**
Main interface for all data operations.

###### `__init__(database_url)`
- **What it does:** Initialize engine with schema builder and normalizer
- **Impact:** Sets up components for data operations

---

###### `initialize()`
- **What it does:** Complete engine setup
  1. Build SQL schema (creates models & tables)
  2. Load metadata files
  3. Set up table relationships
  4. Open database session
- **Impact:** Engine is ready to insert/query data

---

###### `insert_record(record)`
- **What it does:** Insert a complete nested record
- **Steps:**
  1. Normalize record (decompose into root + nested)
  2. Insert root record and get its ID
  3. For each nested table, insert its records with FK pointing to root ID
  4. Commit all changes atomically
- **Error Handling:** Rollback on any failure
- **Impact:** Single nested JSON becomes multiple related SQL rows

---

###### `bulk_insert_from_file(json_file)`
- **What it does:** Load JSON array and insert all records
- **Process:**
  ```
  For each record in json_file:
      Call insert_record()
      Track success/failure counts
      Log progress every 100 records
  ```
- **Returns:** `(success_count, fail_count)`
- **Impact:** Batch inserts `sql_data.json` efficiently

---

###### `query_all(table_name, limit)`
- **What it does:** Fetch records from specified table
- **Impact:** Access individual tables (useful for queries)

---

###### `query_by_id(table_name, record_id)`
- **What it does:** Fetch single record by ID
- **Impact:** Direct record lookup

---

###### `get_record_with_nested(record_id)`
- **What it does:** Reconstruct original nested structure
- **Process:**
  1. Fetch root record from main_records
  2. For each child table, fetch all records with matching FK
  3. Reassemble into nested JSON
- **Example:**
  ```python
  # Database has:
  main_records: {id: 1, name: "John", email: "john@example.com"}
  main_records_tags: {id: 1, main_records_id: 1, value: "important", position: 0}
  main_records_tags: {id: 2, main_records_id: 1, value: "urgent", position: 1}
  
  # Returns:
  {
    "id": 1,
    "name": "John",
    "email": "john@example.com",
    "tags": [
      {"id": 1, "value": "important", "position": 0},
      {"id": 2, "value": "urgent", "position": 1}
    ]
  }
  ```
- **Impact:** Reconstruct original format from SQL; enables round-trip testing

---

###### `delete_record(record_id)`
- **What it does:** Delete record and all nested children
- **Steps:**
  1. Find all child records in related tables
  2. Delete them explicitly (safe cascade)
  3. Delete root record
- **Impact:** Maintains referential integrity; no orphaned rows

---

###### `get_table_count(table_name)` & `get_database_stats()`
- **What they do:** Aggregate statistics (row counts per table)
- **Impact:** Monitoring and debugging

---

---

### FILE 3: sql_pipeline.py
**Purpose:** Orchestrate schema creation, data loading, and reporting

#### Functions

##### `run_sql_pipeline(schema_builder, engine)`
- **What it does:** Complete pipeline execution
- **Steps:**
  1. Initialize schema (create tables)
  2. Load `sql_data.json` from router
  3. Bulk insert all records
  4. Generate summary statistics
  5. Print final report
- **Returns:** `(success_count, fail_count)`

- **Output Example:**
  ```
  ════════════════════════════════════════════════════════════════════════════════
  SQL PIPELINE SUMMARY
  ════════════════════════════════════════════════════════════════════════════════
  
  Database: sqlite:////data/engine.db
  
  Load Results:
    Successful Inserts: 950
    Failed Inserts: 50
    Total Processed: 1000
  
  Final Database State:
    main_records                30 records
    main_records_tags           120 records
    main_records_metadata       30 records
    main_records_sensor_data    60 records
  
  Total Records: 240
  ════════════════════════════════════════════════════════════════════════════════
  ```

- **Impact:** End-to-end pipeline reporting

---

##### `create_sample_sql_data()`
- **What it does:** Generate test data if no routed data exists
- **Impact:** Makes testing possible without classifier

---

##### `main()` with Command Router
- **Commands:**
  - `run` (default): Full pipeline
  - `init`: Schema only (no data)
  - `sample`: Create + load test data
  - `status`: Show database statistics
- **Impact:** Flexible CLI interface

---

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    INITIAL INPUT FILES                           │
├─────────────────────────────────────────────────────────────────┤
│ initial_schema.json        metadata_manager.json   sql_data.json │
│   (ground truth)          (analyzer output)       (router output)│
└──────┬──────────────────────┬──────────────────────┬─────────────┘
       │                      │                      │
       ▼                      ▼                      │
┌─────────────────────────────────────────┐        │
│  SchemaAnalyzer.load_schemas()          │        │
│  ├─ Load initial_schema.json            │        │
│  ├─ Parse metadata_manager.json         │        │
│  └─ Index field_metadata.json           │        │
└──────────┬──────────────────────────────┘        │
           │                                       │
           ▼                                       │
┌─────────────────────────────────────────┐        │
│  SchemaAnalyzer.build_table_hierarchy() │        │
│  ├─ Identify root fields                │        │
│  ├─ Find nested objects                 │        │
│  ├─ Detect arrays                       │        │
│  └─ Map parent-child relationships      │        │
└──────────┬──────────────────────────────┘        │
           │                                       │
           ▼                                       │
┌─────────────────────────────────────────┐        │
│  SQLSchemaBuilder._create_models()      │        │
│  ├─ MainRecords (root model)            │        │
│  ├─ Nested object models                │        │
│  └─ Array junction models               │        │
└──────────┬──────────────────────────────┘        │
           │                                       │
           ▼                                       │
┌─────────────────────────────────────────┐        │
│  SQLSchemaBuilder._create_tables()      │        │
│  └─ CREATE TABLE ... (SQLite/MySQL)     │        │
└──────────┬──────────────────────────────┘        │
           │                                       │
     ┌─────┴────────────────────────────────┬──────┴──────────┐
     │                                      │                 │
     ▼                                      ▼                 │
  [DATABASE]                    ┌────────────────────────────┐│
  ├─ main_records              │ sql_pipeline.run()         ││
  │  (root fields)             │                            ││
  ├─ main_records_*            │ 1. Initialize schema ✓     ││
  │  (nested objects)          │ 2. Load sql_data.json ◄────┘│
  ├─ main_records_*            │ 3. Bulk insert [1000 records]
  │  (array junctions)         │                            │
  └─                           │    For each record:       │
                               │    ├─ normalize_record() │
                               │    ├─ insert_record()    │
                               │    └─ track success/fail │
                               │                            │
                               │ 4. get_database_stats()   │
                               │                            │
                               └────────┬─────────────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │  FINAL REPORT    │
                              │  Statistics      │
                              │  Table counts    │
                              │  Success/fail    │
                              └──────────────────┘
```

---

## Key Functions & Their Impact

### Execution Flow & Impact Chain

```
User runs: python sql_pipeline.py run

↓ (calls main())

↓

↓ (parser identifies 'run' command)

↓

run_sql_pipeline(schema_builder, engine)

    │
    ├─ engine.initialize()
    │   ├─ schema_builder.analyze_and_build()
    │   │   ├─ SchemaAnalyzer.load_schemas() → load metadata
    │   │   ├─ SchemaAnalyzer.build_table_hierarchy() → map relationships
    │   │   ├─ SQLSchemaBuilder._create_models() → create ORM models
    │   │   └─ SQLSchemaBuilder._create_tables() → CREATE TABLE in SQLite
    │   │
    │   ├─ normalizer.load_metadata() → remember is_nested, is_array
    │   └─ build_relationships() → populate parent-child map
    │
    │   [DATABASE NOW HAS EMPTY TABLES]
    │
    ├─ Load sql_data.json
    │   Read up to 1000 records from sql_data.json (from router)
    │
    ├─ engine.bulk_insert_from_file(sql_data.json)
    │   For each of 1000 records:
    │       │
    │       ├─ DataNormalizer.normalize_record()
    │       │   ├─ Separate root fields → {name, email, ...}
    │       │   ├─ Separate nested objects → {metadata: {...}}
    │       │   └─ Separate arrays → {tags: ["a", "b"], ...}
    │       │
    │       ├─ SQLEngine.insert_record()
    │       │   ├─ Insert root record → main_records (get ID)
    │       │   ├─ Insert metadata record → main_records_metadata (FK=ID)
    │       │   ├─ Insert tag records → main_records_tags (FK=ID, each row)
    │       │   └─ Commit atomically
    │       │
    │       └─ Track success [incremented by 1]
    │
    │   [DATABASE NOW HAS ~1000+ ROWS ACROSS ALL TABLES]
    │
    └─ engine.get_database_stats()
        ├─ Count rows in main_records → 950
        ├─ Count rows in main_records_tags → 2850
        ├─ Count rows in main_records_metadata → 950
        └─ Print summary report

↓

SUCCESS ✓
```

### Impact Summary

| Function | Input | Output | Impact |
|----------|-------|--------|--------|
| **load_schemas()** | JSON files | Metadata dict | Database understands schema |
| **build_table_hierarchy()** | Metadata | Relationship map | Foreign keys defined |
| **_create_models()** | Relationships | SQLAlchemy classes | ORM ready |
| **_create_tables()** | Models | Database tables | Schema materialized |
| **normalize_record()** | Nested JSON | (root, nested_map) | Data decomposed |
| **insert_record()** | Root + nested | Record ID | Data normalized & stored |
| **bulk_insert_from_file()** | JSON array | (success, fail) | Entire dataset loaded |
| **get_database_stats()** | Models | Table counts | Monitoring/validation |

---

## Example Walkthrough

### Scenario: Inserting a Complex Record

**Input Record (from sql_data.json):**
```json
{
  "username": "alice123",
  "email": "alice@example.com",
  "activity_level": 5,
  "metadata": {
    "sensor_data": {
      "temperature": 24.5,
      "readings": [10, 15, 22]
    }
  },
  "tags": ["verified", "premium"],
  "internal_id": "ID-101-ABC"
}
```

**Assuming initial_schema.json defined:**
```json
{
  "username": "string",
  "email": "string",
  "activity_level": "int",
  "metadata": "object",
  "tags": "array",
  "internal_id": "string"
}
```

**Assuming metadata_manager.json flagged:**
```json
{
  "metadata": {"is_nested": true, "is_array": false},
  "tags": {"is_nested": false, "is_array": true, "array_content_type": "primitive"}
}
```

### Step 1: Schema Creation (sql_schema_definer.py)

**SchemaAnalyzer identifies:**
- Root fields: `username`, `email`, `activity_level`, `internal_id`
- Nested objects: `metadata`
- Arrays: `tags`

**SQLSchemaBuilder creates models:**

```python
# main_records table
CREATE TABLE main_records (
  id INTEGER PRIMARY KEY,
  username VARCHAR(255),
  email VARCHAR(255),
  activity_level INTEGER,
  internal_id VARCHAR(255)
);

# nested object table
CREATE TABLE main_records_metadata (
  id INTEGER PRIMARY KEY,
  main_records_id INTEGER NOT NULL FOREIGN KEY,
  sensor_data JSON
);

# array junction table
CREATE TABLE main_records_tags (
  id INTEGER PRIMARY KEY,
  main_records_id INTEGER NOT NULL FOREIGN KEY,
  value VARCHAR(255),
  value_type VARCHAR(50),
  position INTEGER
);
```

### Step 2: Data Insertion (sql_engine.py)

**normalize_record() decomposes:**

```python
root_data = {
  "username": "alice123",
  "email": "alice@example.com",
  "activity_level": 5,
  "internal_id": "ID-101-ABC"
}

nested_data = {
  "main_records_metadata": [
    {"sensor_data": {"temperature": 24.5, "readings": [10, 15, 22]}}
  ],
  "main_records_tags": [
    {"value": "verified", "value_type": "str", "position": 0},
    {"value": "premium", "value_type": "str", "position": 1}
  ]
}
```

**insert_record() executes:**

```
INSERT INTO main_records (username, email, activity_level, internal_id)
VALUES ('alice123', 'alice@example.com', 5, 'ID-101-ABC')
→ Returns ID = 1

INSERT INTO main_records_metadata (main_records_id, sensor_data)
VALUES (1, '{"temperature": 24.5, "readings": [10, 15, 22]}')

INSERT INTO main_records_tags (main_records_id, value, value_type, position)
VALUES (1, 'verified', 'str', 0)

INSERT INTO main_records_tags (main_records_id, value, value_type, position)
VALUES (1, 'premium', 'str', 1)

COMMIT  ← All 4 inserts succeed atomically
```

**Database state after:**
```
main_records:
  id | username   | email             | activity_level | internal_id
  1  | alice123   | alice@example.com | 5              | ID-101-ABC

main_records_metadata:
  id | main_records_id | sensor_data
  1  | 1               | {"temperature": 24.5, "readings": [10, 15, 22]}

main_records_tags:
  id | main_records_id | value    | value_type | position
  1  | 1               | verified | str        | 0
  2  | 1               | premium  | str        | 1
```

### Step 3: Retrieval & Reconstruction

**Query original record:**
```python
engine.get_record_with_nested(1)
```

**Reconstruction process:**
1. Fetch from `main_records` where `id=1`
2. Fetch from `main_records_metadata` where `main_records_id=1`
3. Fetch from `main_records_tags` where `main_records_id=1` ORDER BY position
4. Reassemble:

```json
{
  "id": 1,
  "username": "alice123",
  "email": "alice@example.com",
  "activity_level": 5,
  "internal_id": "ID-101-ABC",
  "metadata": [
    {
      "id": 1,
      "main_records_id": 1,
      "sensor_data": {"temperature": 24.5, "readings": [10, 15, 22]}
    }
  ],
  "tags": [
    {"id": 1, "main_records_id": 1, "value": "verified", "position": 0},
    {"id": 2, "main_records_id": 1, "value": "premium", "position": 1}
  ]
}
```

---

## Overall System Impact

### What This Engine Achieves

1. **Automatic Normalization**
   - Takes messy nested JSON
   - Produces properly normalized SQL schema
   - No manual SQL writing required

2. **Flexibility**
   - Handles arbitrary nesting depth
   - Supports arrays of primitives and objects
   - Heterogeneous records (different structures)

3. **Referential Integrity**
   - Foreign keys enforce consistency
   - Cascading deletes prevent orphaned rows
   - Database constraints instead of application logic

4. **Data Recovery**
   - Can reconstruct original nested format
   - Preserves array order via position
   - Full round-trip fidelity

5. **Scalability**
   - Bulk insert optimization (batch processing)
   - Progress tracking every 100 records
   - Atomic transactions prevent partial inserts

6. **Observability**
   - Table statistics (row counts)
   - Success/failure counts
   - Detailed error logging

### Why This Approach is Superior

| Traditional Approach | This Engine |
|---------------------|------------|
| Manual SQL schema creation | Auto-generated from metadata |
| Handle each nesting case differently | Unified algorithm for all cases |
| JSON storage in BLOB columns | Proper normalized tables |
| No referential integrity | Foreign keys enforce consistency |
| Data scattered across formats | Centralized, queryable tables |
| Hard to validate data | SQL constraints validate automatically |

### Key Achievements

✅ **Problem Solved:** Hybrid database handling (semi-structured + structured)  
✅ **Automation:** Zero manual SQL  
✅ **Correctness:** Referential integrity enforced at database level  
✅ **Performance:** Bulk inserts with progress tracking  
✅ **Debuggability:** Detailed statistics and error messages  
✅ **Flexibility:** Handles diverse data structures  

---

## Summary

The SQL Engine is a **metadata-driven schema generator + data normalizer** that transforms messy nested JSON into clean, queryable relational tables while maintaining full referential integrity and the ability to reconstruct the original format.

**Three-layer architecture:**
1. **Schema Definition**: Metadata → SQLAlchemy Models → SQL Tables
2. **Data Normalization**: Nested JSON → Decomposed Records → Inserted Rows
3. **Pipeline Orchestration**: File I/O → Bulk Operations → Statistics

**Core insight:** By understanding field metadata (is_nested, is_array, content_type), we can automatically place data in the right table structure with proper FKs and constraints, eliminating manual schema writing while maintaining database integrity.
