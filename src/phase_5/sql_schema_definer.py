"""
SQL Schema Definer Module
Purpose: Define SQL schema with proper PK/FK relationships from metadata.json.

Responsibilities:
1. Load metadata.json (final metadata from classifier)
2. Parse field structures to identify:
   - Root-level fields (become columns in main table)
   - Nested objects (become separate tables with FK relationships)
   - Arrays (array items stored in junction tables)
3. Create SQLAlchemy models
4. Enforce PK/FK constraints
5. Create actual database tables

Primary Key Strategy:
  - 'record_id' is always used as PK — it is injected by cleaner.py as a
    guaranteed unique integer index across all ingested records.
  - Child tables (nested/array) always get a surrogate auto-increment PK
    and a FK back to main_records.record_id.
"""

import json
import os
from typing import Dict, Any, Optional

try:
    from sqlalchemy import (
        create_engine, Column, Integer, String, Float, Boolean, DateTime,
        ForeignKey, JSON, inspect, UniqueConstraint
    )
    from sqlalchemy.orm import declarative_base, Session
except ImportError:
    print("[!] SQLAlchemy not installed. Install with: pip install sqlalchemy")
    raise

from src.config import DATA_DIR, METADATA_FILE, DATABASE_URL

Base = declarative_base()


class SchemaAnalyzer:
    """Analyzes metadata to determine table structure"""

    def __init__(self):
        self.metadata = {}
        self.table_hierarchy = {}

    def load_schemas(self):
        if not os.path.exists(METADATA_FILE):
            raise FileNotFoundError(f"Metadata file not found: {METADATA_FILE}")

        with open(METADATA_FILE, 'r') as f:
            data = json.load(f)
            self.metadata = {
                field['field_name']: field
                for field in data.get('fields', [])
            }

    def _map_type_to_sql(self, python_type: str) -> Any:
        type_str = str(python_type).lower()
        type_map = {
            "string": String(255),
            "int": Integer,
            "integer": Integer,
            "float": Float,
            "bool": Boolean,
            "boolean": Boolean,
            "datetime": DateTime,
            "date": DateTime,
        }
        if type_str in type_map:
            return type_map[type_str]
        if len(type_str) > 50 or type_str.startswith("http"):
            return String(500)
        return String(255)

    def get_root_fields(self) -> Dict[str, Dict]:
        """Root-level SQL fields only — excludes nested, arrays, and non-SQL fields."""
        root_fields = {}
        for field_name, field_meta in self.metadata.items():
            if field_meta.get('nesting_depth', 0) != 0:
                continue
            if field_meta.get('is_nested') or field_meta.get('is_array'):
                continue
            if field_meta.get('decision') != 'SQL':
                continue
            root_fields[field_name] = field_meta
        return root_fields

    def get_nested_objects(self) -> Dict[str, Dict]:
        """Nested object fields classified as SQL."""
        return {
            name: meta for name, meta in self.metadata.items()
            if meta.get('is_nested') and not meta.get('is_array')
            and meta.get('decision') == 'SQL'
        }

    def get_arrays(self) -> Dict[str, Dict]:
        """Array fields classified as SQL."""
        return {
            name: meta for name, meta in self.metadata.items()
            if meta.get('is_array') and meta.get('decision') == 'SQL'
        }

    def build_table_hierarchy(self):
        self.table_hierarchy['main_records'] = []

        for field_name in self.get_nested_objects():
            self.table_hierarchy['main_records'].append({
                'field_name': field_name,
                'type': 'nested_object',
                'table_name': f"main_records_{field_name}".replace('.', '_')
            })

        for field_name in self.get_arrays():
            self.table_hierarchy['main_records'].append({
                'field_name': field_name,
                'type': 'array',
                'table_name': f"main_records_{field_name}".replace('.', '_')
            })


class SQLSchemaBuilder:

    def __init__(self, database_url: str = None):
        if database_url is None:
            database_url = DATABASE_URL  # from config — reads POSTGRES_URI env var

        self.database_url = database_url
        self.engine = None
        self.analyzer = SchemaAnalyzer()
        self.models = {}

    def analyze_and_build(self):
        print("[*] Loading metadata...")
        self.analyzer.load_schemas()

        print("[*] Building table hierarchy...")
        self.analyzer.build_table_hierarchy()

        print("[*] Creating SQLAlchemy models...")
        Base.metadata.clear()  # prevents duplicate table error when called multiple times in same process
        self._create_models()

        print("[*] Creating database engine...")
        self.engine = create_engine(self.database_url, echo=False)

        print("[*] Creating database tables...")
        self._create_tables()

    def _create_models(self):
        self._create_main_table()

        for field_name in self.analyzer.get_nested_objects():
            self._create_nested_table(field_name)

        for field_name in self.analyzer.get_arrays():
            self._create_array_table(field_name)

    def _create_main_table(self):
        root_fields = self.analyzer.get_root_fields()

        attrs = {
            '__tablename__': 'main_records',
            # record_id is injected by cleaner.py — guaranteed unique integer
            'record_id': Column(Integer, primary_key=True, nullable=False),
        }

        for field_name, meta in root_fields.items():
            if field_name == 'record_id':
                continue  # already added as PK above
            sql_type = self.analyzer._map_type_to_sql(meta.get('dominant_type', 'string'))
            
            #applying metadata-based contraints
            is_not_null = meta.get('frequency') == 1.0
            is_unique = meta.get('cardinality') == 1.0
            
            attrs[field_name] = Column(sql_type, nullable=not is_not_null, unique = is_unique)

        self.models['main_records'] = type('MainRecords', (Base,), attrs)

    def _create_nested_table(self, field_name: str):
        table_name = f"main_records_{field_name}".replace('.', '_')

        attrs = {
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True, autoincrement=True),
            'main_records_id': Column(Integer, ForeignKey('main_records.record_id'), nullable=False),
        }

        for sub_name, meta in self.analyzer.metadata.items():
            if meta.get('parent_path') == field_name and not meta.get('is_nested') and not meta.get('is_array') and meta.get('decision') == 'SQL':
                col_name = sub_name.split('.')[-1]
                sql_type = self.analyzer._map_type_to_sql(meta.get('dominant_type', 'string'))
                
                is_not_null = meta.get('frequency') == 1.0
                is_unique = meta.get('cardinality') == 1.0
                
                attrs[col_name] = Column(sql_type, nullable=not is_not_null, unique = is_unique)

        self.models[table_name] = type(table_name.capitalize(), (Base,), attrs)

    def _create_array_table(self, field_name: str):
        table_name = f"main_records_{field_name}".replace('.', '_')

        attrs = {
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True, autoincrement=True),
            'main_records_id': Column(Integer, ForeignKey('main_records.record_id'), nullable=False),
            'position': Column(Integer, nullable=True),
        }

        field_meta = self.analyzer.metadata.get(field_name, {})
        if field_meta.get('array_content_type') == 'object':
            # 1NF constraint: Flatten array objects into atomic columns rather than JSON
            for sub_name, meta in self.analyzer.metadata.items():
                if meta.get('parent_path') == field_name and not meta.get('is_nested') and not meta.get('is_array') and meta.get('decision') == 'SQL':
                    col_name = sub_name.split('.')[-1]
                    sql_type = self.analyzer._map_type_to_sql(meta.get('dominant_type', 'string'))
                    
                    is_not_null = meta.get('frequency') == 1.0
                    is_unique = meta.get('cardinality') == 1.0
                    
                    attrs[col_name] = Column(sql_type, nullable=not is_not_null, unique = is_unique)
        else:
            attrs['value'] = Column(String(255), nullable=False)
            attrs['value_type'] = Column(String(50), nullable=True)

        self.models[table_name] = type(table_name.capitalize(), (Base,), attrs)

    def _create_tables(self):
        """Creates all tables in the database."""
        Base.metadata.create_all(self.engine)
        print(f"[+] Database schema created at: {self.database_url}")

        inspector = inspect(self.engine)
        print("\n[INFO] Created tables:")
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            fks = inspector.get_foreign_keys(table_name)
            pk = inspector.get_pk_constraint(table_name)
            print(f"  {table_name}")
            print(f"    columns : {[col['name'] for col in columns]}")
            print(f"    PK      : {pk.get('constrained_columns', [])}")
            for fk in fks:
                print(f"    FK      : {fk['constrained_columns']} → {fk['referred_table']}.{fk['referred_columns']}")

    def get_session(self):
        from sqlalchemy.orm import sessionmaker
        return sessionmaker(bind=self.engine)()

    def get_models(self):
        return self.models


def run_schema_definition():
    print("\n" + "=" * 80)
    print("SQL SCHEMA DEFINER")
    print("=" * 80)

    try:
        builder = SQLSchemaBuilder()
        builder.analyze_and_build()

        print("\n" + "=" * 80)
        print("[SUCCESS] SQL Schema definition complete!")
        print("=" * 80)
        print(f"\nDatabase URL    : {builder.database_url}")
        print(f"Tables created  : {list(builder.models.keys())}")

        return builder

    except FileNotFoundError as e:
        print(f"[!] Error: {e}")
        print("[!] Please ensure metadata.json exists (run 'python main.py initialise' first).")
        return None
    except Exception as e:
        print(f"[!] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    run_schema_definition()