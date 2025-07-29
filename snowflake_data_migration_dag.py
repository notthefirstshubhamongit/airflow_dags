# --- Airflow DAG (migration_dag.py) ---
# This file will be placed in your Airflow DAGs folder.
# Install dependencies in your Airflow environment:
# pip install apache-airflow sqlalchemy snowflake-connector-python pandas pyodbc oracledb

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin # Used for logger in classes
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import snowflake.connector
from snowflake.sqlalchemy import URL
from snowflake.connector.pandas_tools import write_pandas
import pyodbc
#import oracledb # Include if Oracle support is needed later
import json
import io
from datetime import datetime
import logging
import re
import time
from typing import Dict, List, Tuple, Any, Optional
import ast
import uuid # For generating unique IDs

# Configure logging for the DAG
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Re-used Classes from Streamlit Application ===
# These classes are copied directly from your Streamlit code to ensure they are
# available within the Airflow worker environment. Modifications are made to use
# `logger.error` instead of `st.error` and to re-raise exceptions for Airflow
# to handle task failures.

class DatabaseConnector(LoggingMixin): # Inherit LoggingMixin for direct logger access
    """Handle database connections for different sources."""

    @staticmethod
    def connect_oracle(host: str, port: str, service_name: str, username: str, password: str):
        """Connect to Oracle database using SQLAlchemy with cx_Oracle dialect."""
        try:
            connection_string = (
                f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}"
            )
            engine = create_engine(connection_string)
        
            with engine.connect() as connection:
                connection.execute(text("SELECT 1 FROM DUAL")) # Test connection
            logger.info("Oracle connection successful.")
            return engine
        except Exception as e:
            logger.error(f"Oracle connection error: {str(e)}")
            raise # Re-raise for Airflow task failure

    @staticmethod
    def connect_sqlserver(server: str, database: str, username: str, password: str, port: str = "1433"):
        """Connect to SQL Server database with enhanced error handling and connection test."""
        try:
            clean_server = server.strip()
            clean_database = database.strip()
            clean_username = username.strip()
            clean_password = password.strip()
            clean_port = str(port).strip()

            logger.info(f"Attempting SQL Server connection to {clean_server}:{clean_port}/{clean_database}")
            logger.info(f"Using username: {clean_username}")

            # Construct the connection string for pyodbc with ODBC Driver 18
            connection_string = (
                f"mssql+pyodbc://{clean_username}:{clean_password}@{clean_server}:{clean_port}/"
                f"{clean_database}?driver=ODBC+Driver+18+for+SQL Server"
            )

            # Attempt a direct pyodbc connection to test, adding a timeout
            test_conn_string_pyodbc = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={clean_server},{clean_port};"
                f"DATABASE={clean_database};"
                f"UID={clean_username};"
                f"PWD={clean_password}"
            )

            test_conn = pyodbc.connect(test_conn_string_pyodbc, timeout=10) # 10-second timeout
            cursor = test_conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version_info = cursor.fetchone()[0]
            logger.info(f"SQL Server direct connection successful. Version: {version_info.splitlines()[0]}")
            cursor.close()
            test_conn.close()

            # If direct pyodbc connection is successful, create SQLAlchemy engine
            engine = create_engine(connection_string)
            return engine

        except pyodbc.Error as e:
            error_msg = str(e)
            logger.error(f"SQL Server Database Error: {error_msg}")
            # Provide more specific guidance based on common pyodbc errors
            if "Login failed for user" in error_msg or "Authentication failed" in error_msg:
                logger.error("SQL Server authentication failed. Check username and password.")
            elif "Connection timed out" in error_msg or "A network-related or instance-specific error occurred" in error_msg:
                logger.error("SQL Server connection timed out or network error. Check server, port, firewall, and ODBC driver.")
            raise # Re-raise for Airflow task failure
        except Exception as e:
            error_msg = f"Unexpected error during Snowflake connection: {str(e)}"
            logger.error(error_msg)
            raise # Re-raise for Airflow task failure

    @staticmethod
    def connect_snowflake(account: str, username: str, password: str, warehouse: str, database: str, schema: str):
        """Connect to Snowflake with enhanced error handling"""
        try:
            clean_account = account.strip()
            clean_username = username.strip()
            clean_password = password.strip()

            logger.info(f"Attempting Snowflake connection with account: {clean_account}")
            logger.info(f"Using username: {clean_username}")
            logger.info(f"Database: {database}, Schema: {schema}, Warehouse: {warehouse}")

            connection_string = URL(
                account=clean_account,
                user=clean_username,
                password=clean_password,
                database=database,
                schema=schema,
                warehouse=warehouse
            )

            # Test connection first with raw snowflake connector for better error messages
            test_conn = snowflake.connector.connect(
                account=clean_account,
                user=clean_username,
                password=clean_password,
                database=database,
                schema=schema,
                warehouse=warehouse,
                client_session_keep_alive=True
            )

            cursor = test_conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = cursor.fetchone()
            logger.info(f"Snowflake connection successful - User: {result[0]}, Role: {result[1]}, DB: {result[2]}, Schema: {result[3]}")
            cursor.close()
            test_conn.close()

            # Now create SQLAlchemy engine
            engine = create_engine(connection_string)
            return engine

        except snowflake.connector.errors.DatabaseError as e:
            error_msg = str(e)
            logger.error(f"Snowflake Database Error: {error_msg}")
            if "Incorrect username or password" in error_msg:
                logger.error("Authentication failed. Check username, password, account, and MFA status.")
            raise # Re-raise for Airflow task failure
        except Exception as e:
            # Removed the extra parenthesis at the end of the f-string
            error_msg = f"Unexpected error during Snowflake connection: {str(e)}"
            logger.error(error_msg)
            raise # Re-raise for Airflow task failure

class SchemaAnalyzer(LoggingMixin):
    """Analyze source database schemas"""

    def __init__(self, engine, db_type: str):
        self.engine = engine
        self.db_type = db_type.lower()

    def get_tables(self) -> List[Dict]:
        """Get all tables from the database"""
        if self.db_type == 'oracle':
            query = """
            SELECT table_name, owner as schema_name, num_rows, blocks, avg_row_len
            FROM all_tables
            WHERE owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'APEX_030200', 'FLOWS_FILES')
            ORDER BY owner, table_name
            """
        elif self.db_type == 'sqlserver':
            query = """
            SELECT
                t.name as table_name,
                s.name as schema_name,
                p.rows as num_rows,
                CAST(SUM(a.total_pages) * 8 AS BIGINT) as table_size_kb
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.indexes i ON t.object_id = i.object_id
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE s.name NOT IN ('sys', 'information_schema')
            GROUP BY t.name, s.name, p.rows
            ORDER BY s.name, t.name
            """
        else:
            logger.warning(f"Unsupported database type for get_tables: {self.db_type}. Defaulting to generic query.")
            query = "SELECT table_name, table_schema as schema_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [row._asdict() for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching tables from {self.db_type}: {str(e)}")
            raise

    def get_table_columns(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get column information for a specific table"""
        if self.db_type == 'oracle':
            query = """
            SELECT
                column_name, data_type, data_length, data_precision, data_scale,
                nullable, data_default, column_id
            FROM all_tab_columns
            WHERE owner = :schema_name AND table_name = :table_name
            ORDER BY column_id
            """
        elif self.db_type == 'sqlserver':
            query = """
            SELECT
                c.name as column_name,
                t.name as data_type,
                c.max_length as data_length,
                c.precision as data_precision,
                c.scale as data_scale,
                CASE WHEN c.is_nullable = 1 THEN 'Y' ELSE 'N' END as nullable,
                dc.definition as data_default,
                c.column_id
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            WHERE c.object_id = OBJECT_ID(:schema_name + '.' + :table_name)
            ORDER BY c.column_id
            """
        else:
            logger.warning(f"Unsupported database type for get_table_columns: {self.db_type}.")
            # Fallback for other DB types using SQLAlchemy's inspector
            from sqlalchemy import inspect
            inspector = inspect(self.engine)
            columns_info = inspector.get_columns(table_name, schema=schema_name)
            # Adapt to expected dict format, default values might need more specific handling
            return [{
                'column_name': col['name'],
                'data_type': str(col['type']), # Convert SQLAlchemy type to string
                'data_length': col.get('length'),
                'data_precision': col.get('precision'),
                'data_scale': col.get('scale'),
                'nullable': 'Y' if col.get('nullable') else 'N',
                'data_default': col.get('default'),
                'column_id': i + 1 # Assign sequential ID
            } for i, col in enumerate(columns_info)]


        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {
                    'schema_name': schema_name,
                    'table_name': table_name
                })
                return [row._asdict() for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching columns from {schema_name}.{table_name}: {str(e)}")
            raise

    def get_indexes(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get index information for a specific table"""
        if self.db_type == 'oracle':
            query = """
            SELECT
                i.index_name, i.index_type, i.uniqueness,
                ic.column_name, ic.column_position
            FROM all_indexes i
            JOIN all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner
            WHERE i.owner = :schema_name AND i.table_name = :table_name
            ORDER BY i.index_name, ic.column_position
            """
        elif self.db_type == 'sqlserver':
            query = """
            SELECT
                i.name as index_name,
                i.type_desc as index_type,
                CASE WHEN i.is_unique = 1 THEN 'UNIQUE' ELSE 'NONUNIQUE' END as uniqueness,
                c.name as column_name,
                ic.key_ordinal as column_position
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID(:schema_name + '.' + :table_name)
            AND i.name IS NOT NULL
            ORDER BY i.name, ic.key_ordinal
            """
        else:
            logger.warning(f"Unsupported database type for get_indexes: {self.db_type}. Skipping index fetching.")
            return [] # Return empty list for unsupported types

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {
                    'schema_name': schema_name,
                    'table_name': table_name
                })
                return [dict(row) for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching indexes from {schema_name}.{table_name}: {str(e)}")
            raise

class DataTypeMapper:
    """Map source database data types to Snowflake data types"""

    ORACLE_TO_SNOWFLAKE = {
        'VARCHAR2': 'VARCHAR', 'NVARCHAR2': 'VARCHAR', 'CHAR': 'CHAR', 'NCHAR': 'CHAR',
        'CLOB': 'VARCHAR', 'NCLOB': 'VARCHAR', 'NUMBER': 'NUMBER', 'BINARY_FLOAT': 'FLOAT',
        'BINARY_DOUBLE': 'DOUBLE', 'DATE': 'TIMESTAMP_NTZ', 'TIMESTAMP': 'TIMESTAMP_NTZ',
        'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP_TZ', 'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP_LTZ',
        'RAW': 'BINARY', 'BLOB': 'BINARY', 'XMLTYPE': 'VARIANT'
    }

    SQLSERVER_TO_SNOWFLAKE = {
        'varchar': 'VARCHAR', 'nvarchar': 'VARCHAR', 'char': 'CHAR', 'nchar': 'CHAR',
        'text': 'VARCHAR', 'ntext': 'VARCHAR', 'int': 'INTEGER', 'bigint': 'BIGINT',
        'smallint': 'SMALLINT', 'tinyint': 'TINYINT', 'bit': 'BOOLEAN',
        'decimal': 'NUMBER', 'numeric': 'NUMBER', 'float': 'FLOAT', 'real': 'REAL',
        'money': 'NUMBER(19,4)', 'smallmoney': 'NUMBER(10,4)', 'datetime': 'TIMESTAMP_NTZ',
        'datetime2': 'TIMESTAMP_NTZ', 'smalldatetime': 'TIMESTAMP_NTZ', 'date': 'DATE',
        'time': 'TIME', 'datetimeoffset': 'TIMESTAMP_TZ', 'binary': 'BINARY',
        'varbinary': 'BINARY', 'image': 'BINARY', 'uniqueidentifier': 'VARCHAR(36)',
        'xml': 'VARIANT'
    }

    @classmethod
    def map_data_type(cls, source_type: str, db_type: str, length: Optional[int] = None, precision: Optional[int] = None, scale: Optional[int] = None) -> str:
        """Map source data type to Snowflake data type"""
        source_type = source_type.upper() if db_type.lower() == 'oracle' else source_type.lower()

        if db_type.lower() == 'oracle':
            mapping = cls.ORACLE_TO_SNOWFLAKE
        else:
            mapping = cls.SQLSERVER_TO_SNOWFLAKE

        base_type = mapping.get(source_type, 'VARCHAR')

        # Handle specific cases with precision/scale/length
        if source_type in ['NUMBER', 'DECIMAL', 'NUMERIC'] and precision is not None:
            if scale is not None and scale > 0:
                return f"NUMBER({precision},{scale})"
            else:
                return f"NUMBER({precision})"
        elif source_type in ['VARCHAR2', 'VARCHAR', 'NVARCHAR2', 'NVARCHAR'] and length is not None and length > 0:
            # Snowflake VARCHAR max length is 16MB. Adjust if source length is too large.
            # Assuming max_length here for practical purposes.
            return f"VARCHAR({min(length, 16777216)})" # 16MB in bytes
        elif source_type in ['CHAR', 'NCHAR'] and length is not None and length > 0:
            return f"CHAR({min(length, 256)})" # Snowflake CHAR max is 256 characters

        return base_type

class DDLGenerator(LoggingMixin):
    """Generate Snowflake DDL statements"""

    def __init__(self, db_type: str):
        self.db_type = db_type
        self.mapper = DataTypeMapper()

    def generate_create_table_ddl(self, schema_name: str, table_name: str, columns: List[Dict]) -> str:
        """Generate CREATE TABLE DDL for Snowflake"""
        # Ensure schema and table names are correctly quoted for Snowflake if they contain special chars or are case-sensitive
        sf_schema_name = f'"{schema_name.upper()}"'
        sf_table_name = f'"{table_name.upper()}"'

        ddl = f"CREATE OR REPLACE TABLE {sf_schema_name}.{sf_table_name} (\n"

        column_definitions = []
        for col in columns:
            col_name = col['column_name']
            sf_col_name = f'"{col_name.upper()}"' # Quote column names for Snowflake

            data_type = self.mapper.map_data_type(
                col['data_type'],
                self.db_type,
                col.get('data_length'),
                col.get('data_precision'),
                col.get('data_scale')
            )

            nullable = "" if col.get('nullable', 'Y') == 'Y' else " NOT NULL"
            default = ""
            default_val = col.get('data_default')
            if default_val is not None:
                # Basic attempt to format default value. More complex default values might need parsing.
                if isinstance(default_val, str) and not (default_val.startswith("(") and default_val.endswith(")")):
                    default = f" DEFAULT '{default_val}'" # Quote string defaults
                else:
                    default = f" DEFAULT {default_val}" # Assume numeric/function defaults are valid

            column_definitions.append(f"    {sf_col_name} {data_type}{nullable}{default}")

        ddl += ",\n".join(column_definitions)
        ddl += "\n);"
        
        return ddl

    def generate_copy_into_ddl(self, schema_name: str, table_name: str, stage_name: str, file_format: str = 'CSV') -> str:
        """Generate COPY INTO statement"""
        sf_schema_name = f'"{schema_name.upper()}"'
        sf_table_name = f'"{table_name.upper()}"'
        sf_stage_name = f'@{stage_name.upper()}' # Snowflake stage names are typically uppercase

        return f"""
COPY INTO {sf_schema_name}.{sf_table_name}
FROM {sf_stage_name}/{table_name.upper()}/ -- Adjust path if needed
FILE_FORMAT = (TYPE = '{file_format}' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
"""

class MigrationExecutor(LoggingMixin):
    def __init__(self, source_engine, snowflake_engine, batch_size: int = 10000, dest_conn_details: Dict[str, str] = None):
        self.source_engine = source_engine
        self.source_db_type = source_engine.dialect.name.lower()
        self.snowflake_engine = snowflake_engine
        self.batch_size = batch_size

        # Always use dest_conn_details for accurate database and schema
        if not dest_conn_details:
            raise ValueError("dest_conn_details must be provided to MigrationExecutor.")
            
        self.snowflake_database = dest_conn_details.get('database')
        self.snowflake_schema = dest_conn_details.get('schema')
        self.snowflake_warehouse = dest_conn_details.get('warehouse')
        self.snowflake_account = dest_conn_details.get('account')
        self.snowflake_user = dest_conn_details.get('username')

        # Add a check to ensure database and schema are not None
        if not self.snowflake_database or not self.snowflake_schema:
            raise ValueError("Snowflake database or schema not found in destination connection details.")

        logger.info(f"MigrationExecutor initialized for Snowflake DB: {self.snowflake_database}, Schema: {self.snowflake_schema}, Warehouse: {self.snowflake_warehouse}")

    def _get_quoted_table_identifier(self, schema_name: str, table_name: str) -> str:
        """Generates database-specific quoted table identifier (e.g., "SCHEMA"."TABLE" or [SCHEMA].[TABLE])."""
        if self.source_db_type == 'oracle':
            return f"\"{schema_name.upper()}\".\"{table_name.upper()}\""
        elif self.source_db_type == 'sqlserver':
            return f"[{schema_name}].[{table_name}]"
        elif self.source_db_type == 'mysql':
            return f"`{schema_name}`.`{table_name}`"
        elif self.source_db_type == 'postgresql':
            return f'"{schema_name}"."{table_name}"'
        else:
            logger.warning(f"Unsupported source database type for quoting: {self.source_db_type}. Using unquoted identifier.")
            return f"{schema_name}.{table_name}" # Fallback

    def _execute_query(self, query: str, fetch_type: str = 'all'):
        """Execute query using SQLAlchemy engine with proper connection handling"""
        try:
            with self.source_engine.connect() as connection:
                result = connection.execute(text(query))
                if fetch_type == 'one':
                    row = result.fetchone()
                    return row
                elif fetch_type == 'all':
                    return result.fetchall()
                else:
                    return result # Return the result proxy for other operations
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def _safe_read_sql(self, query: str, chunksize: Optional[int] = None):
        """
        Safely read SQL data with multiple fallback methods using pandas.read_sql.
        This attempts various ways to ensure pandas can connect.
        """
        methods_to_try = [
            lambda: pd.read_sql(query, self.source_engine, chunksize=chunksize),
            lambda: self._read_sql_with_connection_begin(query, chunksize),
            lambda: self._read_sql_with_raw_connection(query, chunksize),
            lambda: pd.read_sql(query, str(self.source_engine.url), chunksize=chunksize) # Method 4 directly using connection string
        ]

        last_error = None
        for i, method in enumerate(methods_to_try, 1):
            try:
                logger.info(f"Trying pandas.read_sql method {i}")
                result = method()
                logger.info(f"pandas.read_sql method {i} succeeded")
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"pandas.read_sql method {i} failed: {str(e)}")
                continue

        # If all methods fail, raise the last error
        logger.critical(f"All pandas.read_sql connection methods failed. Last error: {last_error}")
        raise last_error

    def _read_sql_with_connection_begin(self, query: str, chunksize: Optional[int] = None):
        """Method 2: Use connection with transaction (engine.begin())."""
        with self.source_engine.begin() as connection:
            if chunksize:
                return pd.read_sql(query, connection, chunksize=chunksize)
            else:
                return pd.read_sql(query, connection)

    def _read_sql_with_raw_connection(self, query: str, chunksize: Optional[int] = None):
        """Method 3: Use raw connection (engine.raw_connection())."""
        raw_conn = self.source_engine.raw_connection()
        try:
            if chunksize:
                return pd.read_sql(query, raw_conn, chunksize=chunk_size)
            else:
                return pd.read_sql(query, raw_conn)
        finally:
            raw_conn.close()

    # _read_sql_with_connection_string is now inlined into _safe_read_sql's methods_to_try

    def extract_data_chunked(self, schema_name: str, table_name: str,
                             chunk_size: Optional[int] = None):
        """Extract data in chunks using pandas read_sql with chunksize"""
        try:
            chunk_size = chunk_size or self.batch_size

            # Build database-specific query with proper quoting
            quoted_table_identifier = self._get_quoted_table_identifier(schema_name, table_name)
            query = f"SELECT * FROM {quoted_table_identifier}"

            logger.info(f"Starting chunked extraction from {schema_name}.{table_name} with query: {query}")

            # Use safe read method that tries multiple connection approaches
            chunks = self._safe_read_sql(query, chunksize=chunk_size)

            # Handle both generator and direct result from pandas.read_sql
            if hasattr(chunks, '__iter__') and not isinstance(chunks, pd.DataFrame):
                # It's a chunk generator
                for chunk_df in chunks:
                    yield chunk_df
            else:
                # It's a single DataFrame (e.g., if total rows < chunk_size)
                yield chunks

        except Exception as e:
            logger.error(f"Error in chunked extraction from {schema_name}.{table_name}: {str(e)}")
            raise

    def extract_source_data(self, schema_name: str, table_name: str,
                            chunk_size: Optional[int] = None) -> pd.DataFrame:
        """
        Extracts data from the source database table.
        Uses chunked reading for large tables and full load for small ones.
        """
        try:
            chunk_size = chunk_size or self.batch_size

            # Build database-specific query with proper quoting
            quoted_table_identifier = self._get_quoted_table_identifier(schema_name, table_name)
            query = f"SELECT * FROM {quoted_table_identifier}"

            row_count = self.get_table_row_count(schema_name, table_name)
            logger.info(f"Table {schema_name}.{table_name} has {row_count} rows.")

            if row_count == 0:
                logger.info(f"Table {schema_name}.{table_name} is empty. Returning empty DataFrame.")
                return pd.DataFrame() # Return empty DataFrame if no rows

            if row_count <= chunk_size:
                # Small table - load all at once using safe method
                logger.info(f"Loading all {row_count} rows for {schema_name}.{table_name} (small table).")
                df = self._safe_read_sql(query)
                if not isinstance(df, pd.DataFrame): # If safe_read_sql returned a generator with one chunk
                    df = next(df)
            else:
                # Large table - use chunked reading and concatenate
                logger.info(f"Loading {row_count} rows for {schema_name}.{table_name} in chunks of {chunk_size}.")
                df_chunks = []
                for chunk in self.extract_data_chunked(schema_name, table_name, chunk_size):
                    df_chunks.append(chunk)

                if df_chunks:
                    df = pd.concat(df_chunks, ignore_index=True)
                else:
                    df = pd.DataFrame()

            logger.info(f"Extracted {len(df)} rows from {schema_name}.{table_name}")
            return df

        except Exception as e:
            logger.error(f"Error extracting data from {schema_name}.{table_name}: {str(e)}")
            raise

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get row count for a table using database-specific query and quoting."""
        try:
            quoted_table_identifier = self._get_quoted_table_identifier(schema_name, table_name)
            query = f"SELECT COUNT(*) FROM {quoted_table_identifier}"

            logger.info(f"Getting row count for {schema_name}.{table_name} with query: {query}")
            result = self._execute_query(query, 'one')
            row_count = result[0] if result else 0

            return row_count

        except Exception as e:
            logger.error(f"Error getting row count for {schema_name}.{table_name}: {str(e)}")
            return 0 # Return 0 if count fails, let main logic handle 0-row tables

    def transform_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms source database data types to Snowflake compatible types.
        Handles common conversions and NULL values. Also ensures column names are uppercase.
        """
        try:
            df_transformed = df.copy()

            # Convert all column names to uppercase for Snowflake compatibility
            df_transformed.columns = [col.upper() for col in df_transformed.columns]

            for col in df_transformed.columns:
                # Attempt to convert to string where objects are found (e.g., CLOB/BLOB as text)
                if df_transformed[col].dtype == 'object':
                    df_transformed[col] = df_transformed[col].astype(str)
                    # Replace various representations of NaN/None string with actual None
                    df_transformed[col] = df_transformed[col].replace({'nan', '<NA>', 'None'}, None)

                # Handle datetime columns: ensure they are proper datetime objects
                elif pd.api.types.is_datetime64_any_dtype(df_transformed[col]):
                    df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')

                # Handle numeric types: ensure they are numeric, coercing errors to NaN
                elif pd.api.types.is_numeric_dtype(df_transformed[col]):
                    df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')

                # Replace pandas NaN (from coercing errors or missing values) with None for Snowflake compatibility
                df_transformed[col] = df_transformed[col].where(pd.notnull(df_transformed[col]), None)

            logger.info("Data types and column names transformed successfully.")
            return df_transformed
        except Exception as e:
            logger.error(f"Error transforming data types: {str(e)}")
            raise

    def load_data_to_snowflake(self, df: pd.DataFrame, target_schema: str, target_table: str):
        """Loads a pandas DataFrame into a Snowflake table using write_pandas with automatic transaction management from SQLAlchemy."""
        sf_target_schema = target_schema.upper()
        sf_target_table = target_table.upper()
        
        # The `with self.snowflake_engine.connect() as conn:` block handles the transaction.
        # It automatically commits if the block completes without error, and rolls back if an exception occurs.
        with self.snowflake_engine.begin() as conn:
            print("connecting snowflake")
            try:
                logger.info(f"Starting load to Snowflake table {sf_target_schema}.{sf_target_table} with {len(df)} rows.")

                # The .connection attribute gives the underlying DBAPI connection (the snowflake.connector object)
                dbapi_conn = conn.connection 
                
                # Use the DBAPI connection for write_pandas
                print("writing dataframe to snowflake")
                write_result = write_pandas(
                    conn=dbapi_conn,
                    df=df,
                    table_name=sf_target_table,
                    database=self.snowflake_database.upper(),
                    schema=sf_target_schema,
                    chunk_size=self.batch_size,
                    quote_identifiers=True
                )
                print("write result",write_result)

                # Unpack the 4-tuple result from write_pandas
                success, n_chunks, n_rows, _ = write_result
                if not success:
                    raise Exception(f"Failed to load data to Snowflake. write_pandas returned: {write_result}")

                logger.info(f"Successfully staged {n_rows} rows in {n_chunks} chunks for table {sf_target_schema}.{sf_target_table}. Transaction will be committed upon successful exit.")

                # --- VERIFICATION STEP (within the same transaction) ---
                logger.info(f"Verifying row count in Snowflake for {sf_target_schema}.{sf_target_table} before commit...")
                full_table_name = f'"{self.snowflake_database.upper()}"."{sf_target_schema}"."{sf_target_table}"'
                count_query = f'SELECT COUNT(*) FROM {full_table_name}'
                
                logger.info(f"Executing verification query: {count_query}")
                # Use the same SQLAlchemy connection for verification
                result = conn.execute(text(count_query)).scalar()
                print("verification inputs row and len ",result,len(df))
                if result is not None:
                    print("result is not none")
                    logger.info(f"Verification: Table {full_table_name} has {result} rows within the transaction.")
                    if result != len(df):
                        logger.warning(f"Verification Mismatch: Loaded {len(df)} rows, but Snowflake reported {result} rows before commit.")
                        
                    print("result and len(df) matches")
                else:
                    print("else condition")
                    logger.warning(f"Verification: Could not retrieve row count for {full_table_name}.")

            except Exception:
                logger.error("Transaction for %s.%s rolled back due to error.", target_schema, target_table)
                #logger.error(f"Error during Snowflake load for {target_schema}.{target_table}. Transaction will be rolled back. Error: {str(e)}")
                raise # Re-raise the exception to trigger the rollback and fail the Airflow task
        
        logger.info(f"Transaction for {sf_target_schema}.{sf_target_table} completed (committed or rolled back).")
        return True

    def create_snowflake_table(self, target_schema: str, target_table: str, source_schema: str, source_table: str, source_db_type: str):
        """Creates the target table in Snowflake based on source table schema."""
        try:
            analyzer = SchemaAnalyzer(self.source_engine, source_db_type)
            columns = analyzer.get_table_columns(source_schema, source_table)
            if not columns:
                raise ValueError(f"No columns found for source table {source_schema}.{source_table}. Cannot create target table.")

            ddl_gen = DDLGenerator(source_db_type)
            create_table_sql = ddl_gen.generate_create_table_ddl(target_schema, target_table, columns)

            logger.info(f"Generated DDL for {target_schema}.{target_table}:\n{create_table_sql}")

            with self.snowflake_engine.connect() as conn:
                conn.execute(text(create_table_sql))
            logger.info(f"Successfully created table {target_schema}.{target_table} in Snowflake.")
            return True
        except Exception as e:
            logger.error(f"Error creating table {target_schema}.{target_table} in Snowflake: {str(e)}")
            raise

# --- Metadata Logging Function ---
def insert_migration_metadata(
    snowflake_engine,
    source_conn_details: Dict[str, Any],
    dest_conn_details: Dict[str, Any],
    table_info: Dict[str, str],
    status: str,
    task_id: str,
    run_id: str,
    metadata_id: Optional[int] = None # Optional for updates, required for initial insert
):
    """
    Inserts or updates migration metadata into the Snowflake_Migrator_metadata table.
    """
    try:
        # Extract details from parameters
        source_database = source_conn_details.get('database')
        source_schema = table_info.get('source_schema')
        source_table_name = table_info.get('source_table')
        source_username = source_conn_details.get('username')

        destination_database = dest_conn_details.get('database')
        destination_schema = table_info.get('destination_schema')
        destination_table_name = table_info.get('destination_table')
        destination_username = dest_conn_details.get('username')

        # Generate a unique ID if not provided (for initial insert)
        if metadata_id is None:
            # Using current nanosecond timestamp + a small random component for uniqueness
            # This should fit NUMBER(38,0)
            generated_id = int(time.time_ns()) + uuid.uuid4().int % 1000000000 # Add a large random component
            metadata_id = generated_id
        
        # Define the metadata table path
        metadata_table_path = "SNOWFLAKE_MIG.Migration.Snowflake_Migrator_metadata"

        if status == 'STARTED':
            # Insert new record
            insert_sql = f"""
            INSERT INTO {metadata_table_path} (
                ID, SourceDatabase, SourceSchema, SourceTableName, SourceUserName,
                DestinationDatabase, DestinationSchema, DestinationTableName, DestinationUserName,
                Status, TaskID, RunID
            ) VALUES (
                :id, :source_db, :source_schema, :source_table, :source_user,
                :dest_db, :dest_schema, :dest_table, :dest_user,
                :status, :task_id, :run_id
            );
            """
            params = {
                'id': metadata_id,
                'source_db': source_database,
                'source_schema': source_schema,
                'source_table': source_table_name,
                'source_user': source_username,
                'dest_db': destination_database,
                'dest_schema': destination_schema,
                'dest_table': destination_table_name,
                'dest_user': destination_username,
                'status': status,
                'task_id': task_id,
                'run_id': run_id
            }
            logger.info(f"Inserting metadata for ID {metadata_id} with status '{status}'.")
            with snowflake_engine.connect() as conn:
                conn.execute(text(insert_sql), params)
            return metadata_id # Return the ID for subsequent updates
        else:
            # Update existing record (for COMPLETED or FAILED)
            update_sql = f"""
            UPDATE {metadata_table_path}
            SET Status = :status
            WHERE ID = :id;
            """
            params = {
                'status': status,
                'id': metadata_id
            }
            logger.info(f"Updating metadata for ID {metadata_id} to status '{status}'.")
            with snowflake_engine.connect() as conn:
                conn.execute(text(update_sql), params)
            return metadata_id

    except Exception as e:
        logger.error(f"Error inserting/updating migration metadata: {str(e)}")
        # Do not re-raise, as metadata logging should not fail the entire DAG run
        # unless it's critical for downstream tasks. For now, just log.

# --- Airflow DAG Definition ---

def execute_table_migration_task(**context):
    """
    Airflow Python callable to execute the migration for all tables
    specified in the DAG run configuration (`dag_run.conf`).
    """
    dag_run_conf = context['dag_run'].conf
    # Ensure all keys exist before accessing, provide default empty dict if not
    source_conn_details = dag_run_conf.get('source_connection', {})
    dest_conn_details = dag_run_conf.get('destination_connection', {})
    tables_to_migrate_raw = dag_run_conf.get('tables_to_migrate', [])

    task_id = context['task'].task_id
    run_id = context['dag_run'].run_id

    if not source_conn_details or not dest_conn_details or not tables_to_migrate_raw:
        logger.error("Missing required connection details or tables_to_migrate in DAG run configuration.")
        raise ValueError("Invalid DAG run configuration.")

    # Ensure tables_to_migrate_raw is actually a list, and parse if it's a string representation of a list
    tables_to_migrate = []
    if isinstance(tables_to_migrate_raw, str):
        try:
            tables_to_migrate = json.loads(tables_to_migrate_raw)
            if not isinstance(tables_to_migrate, list):
                logger.error(f"dag_run.conf['tables_to_migrate'] was a JSON string but not a list: {tables_to_migrate_raw}")
                raise ValueError("tables_to_migrate is not a valid list after JSON parsing.")
        except json.JSONDecodeError:
            logger.error(f"dag_run.conf['tables_to_migrate'] is a malformed JSON string: {tables_to_migrate_raw}")
            raise ValueError("tables_to_migrate is a malformed JSON string.")
    elif isinstance(tables_to_migrate_raw, list):
        tables_to_migrate = tables_to_migrate_raw
    else:
        logger.error(f"Element in tables_to_migrate is neither a list nor a string: {type(tables_to_migrate_raw)}")
        raise ValueError("tables_to_migrate is in an unexpected format.")


    logger.info(f"Received migration request for {len(tables_to_migrate)} tables.")

    # Establish global connections for the entire task.
    source_engine_global = None
    snowflake_engine_global = None # Initialized to None for safety

    try:
        source_db_type = source_conn_details.get('type', '').lower()
        if source_db_type == 'sqlserver':
            source_engine_global = DatabaseConnector.connect_sqlserver(
                server=source_conn_details.get('host'),
                port=source_conn_details.get('port'),
                database=source_conn_details.get('database'),
                username=source_conn_details.get('username'),
                password=source_conn_details.get('password')
            )
        elif source_db_type == 'oracle':
            source_engine_global = DatabaseConnector.connect_oracle(
                host=source_conn_details.get('host'),
                port=source_conn_details.get('port'),
                service_name=source_conn_details.get('service_name'),
                username=source_conn_details.get('username'),
                password=source_conn_details.get('password')
            )
        else:
            raise ValueError(f"Unsupported source database type: {source_db_type}")

        if not source_engine_global:
            raise Exception("Failed to establish global source database connection.")
        logger.info("Global source database connection established.")

        snowflake_engine_global = DatabaseConnector.connect_snowflake(
            account=dest_conn_details.get('account'),
            username=dest_conn_details.get('username'),
            password=dest_conn_details.get('password'),
            warehouse=dest_conn_details.get('warehouse'),
            database=dest_conn_details.get('database'),
            schema=dest_conn_details.get('schema')
        )
        if not snowflake_engine_global:
            raise Exception("Failed to establish global Snowflake connection.")
        logger.info("Global Snowflake connection established.")

        # Iterate and migrate each table sequentially within this single task
        for i, table_info_item_raw in enumerate(tables_to_migrate):
            table_info_item = None
            current_metadata_id = None # Initialize for each table

            if isinstance(table_info_item_raw, str):
                try:
                    table_info_item = ast.literal_eval(table_info_item_raw)
                    if not isinstance(table_info_item, dict):
                        logger.error(f"Element {i} in tables_to_migrate is a string but not a dict after literal_eval: {table_info_item_raw}")
                        continue
                except (ValueError, SyntaxError) as e:
                    logger.error(f"Element {i} in tables_to_migrate is a malformed Python literal string: {table_info_item_raw}. Error: {e}")
                    continue
            elif isinstance(table_info_item_raw, dict):
                table_info_item = table_info_item_raw
            else:
                logger.error(f"Element {i} in tables_to_migrate is not a dictionary or a string: {table_info_item_raw}")
                raise ValueError("tables_to_migrate is in an unexpected format.")

            if not isinstance(table_info_item, dict):
                logger.error(f"Element {i} in tables_to_migrate could not be processed as a dictionary: {table_info_item_raw}")
                continue

            # Now, table_info_item is guaranteed to be a dictionary
            source_schema = table_info_item.get('source_schema')
            source_table = table_info_item.get('source_table')
            destination_schema = table_info_item.get('destination_schema')
            destination_table = table_info_item.get('destination_table')

            if not all([source_schema, source_table, destination_schema, destination_table]):
                logger.error(f"Skipping migration for table entry at index {i} due to missing required fields: {table_info_item}")
                continue

            logger.info(f"--- Starting migration for table {i+1}/{len(tables_to_migrate)}: "
                        f"{source_schema}.{source_table} -> {destination_schema}.{destination_table} ---")
            
            try:
                # Log 'STARTED' status for metadata
                current_metadata_id = insert_migration_metadata(
                    snowflake_engine=snowflake_engine_global,
                    source_conn_details=source_conn_details,
                    dest_conn_details=dest_conn_details,
                    table_info=table_info_item,
                    status='STARTED',
                    task_id=task_id,
                    run_id=run_id,
                    metadata_id=None # Let the function generate a new ID
                )

                # Pass dest_conn_details to MigrationExecutor
                migration_executor = MigrationExecutor(source_engine_global, snowflake_engine_global, dest_conn_details=dest_conn_details)

                logger.info(f"Creating table {destination_schema}.{destination_table} in Snowflake...")
                migration_executor.create_snowflake_table(destination_schema, destination_table, source_schema, source_table, source_db_type)

                logger.info(f"Extracting data from {source_schema}.{source_table}...")
                df = migration_executor.extract_source_data(source_schema, source_table)
                logger.info(f"Transforming data types for {source_schema}.{source_table}...")
                df_transformed = migration_executor.transform_data_types(df)
               # print(df_transformed)

                if not df_transformed.empty:
                    logger.info(f"Loading {len(df_transformed)} rows to Snowflake table {destination_schema}.{destination_table}...")
                    print("Calling load_data_to_snowflake")
                    migration_executor.load_data_to_snowflake(df_transformed, destination_schema, destination_table)
                    print("after load_data_to_snowflake")
                else:
                    logger.info(f"No data to load for {source_schema}.{source_table}, skipping load step.")

                logger.info(f"Migration for {source_schema}.{source_table} completed successfully.")
                
                # Log 'COMPLETED' status for metadata
                insert_migration_metadata(
                    snowflake_engine=snowflake_engine_global,
                    source_conn_details=source_conn_details,
                    dest_conn_details=dest_conn_details,
                    table_info=table_info_item,
                    status='COMPLETED',
                    task_id=task_id,
                    run_id=run_id,
                    metadata_id=current_metadata_id # Use the ID generated at 'STARTED'
                )

            except Exception as e:
                logger.error(f"Migration FAILED for table {source_schema}.{source_table}: {str(e)}")
                # Log 'FAILED' status for metadata
                if current_metadata_id: # Only attempt update if an ID was generated
                    insert_migration_metadata(
                        snowflake_engine=snowflake_engine_global,
                        source_conn_details=source_conn_details,
                        dest_conn_details=dest_conn_details,
                        table_info=table_info_item,
                        status='FAILED',
                        task_id=task_id,
                        run_id=run_id,
                        metadata_id=current_metadata_id # Use the ID generated at 'STARTED'
                    )
                raise # Re-raise to mark the Airflow task as failed

        logger.info("All selected tables processed in the migration DAG.")

    finally:
        print("close connection")
        # Close connections if they were opened
        if source_engine_global:
            source_engine_global.dispose()
            logger.info("Global source database engine disposed.")

        if snowflake_engine_global:
            snowflake_engine_global.dispose()
            logger.info("Global Snowflake engine disposed.")

# Define the Airflow DAG
with DAG(
    dag_id='snowflake_data_migration_dag',
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval=None,
    catchup=False,
    tags=['data_migration', 'snowflake', 'sqlserver', 'fastapi'],
    params={
        "source_connection": {
            "type": "sqlserver",
            "host": "your_sql_server_host_example",
            "port": "1433",
            "database": "sample_source_db",
            "username": "sample_source_user",
            "password": "sample_source_password"
        },
        "destination_connection": {
            "type": "snowflake",
            "account": "your_sf_account_example",
            "warehouse": "your_sf_warehouse_example",
            "database": "sample_target_db",
            "schema": "SAMPLE_SCHEMA",
            "username": "sample_sf_user",
            "password": "sample_sf_password"
        },
        "tables_to_migrate": [
            {"source_schema": "dbo", "source_table": "YourTable1", "destination_schema": "PUBLIC", "destination_table": "YOURTABLE1_MIGRATED"},
            {"source_schema": "dbo", "source_table": "YourTable2", "destination_schema": "PUBLIC", "destination_table": "YOURTABLE2_MIGRATED"}
        ]
    }
) as dag:
    migrate_data_task = PythonOperator(
        task_id='execute_full_migration',
        python_callable=execute_table_migration_task,
        provide_context=True,
        dag=dag,
    )
