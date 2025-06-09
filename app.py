import streamlit as st
from kbcstorage.client import Client
# Attempting to import StorageApiError from client, if not found, will rely on general Exception
try:
    from kbcstorage.client import StorageApiError
except ImportError:
    StorageApiError = Exception # Fallback to a general exception if specific one not found

import pandas as pd
import snowflake.connector
from google.cloud import bigquery
from google.oauth2 import service_account # Will be used for parsing JSON from text area
import json
import tempfile
import os
import numpy as np

# Import the new UI and state management functions
from config_ui import render_configuration_ui
from session_state_manager import initialize_session_state
from comparison_logic import execute_comparison_workflow, apply_rounding_and_process_comparison # Import the new workflow and rounding functions

# Initialize session state by calling the function from the new module
initialize_session_state()

st.set_page_config(layout="wide")

# --- Constants ---
# DEFAULT_KBC_URL = "https://connection.keboola.com" # Will be an input now

# --- Session State Initialization ---
if 'origin_df' not in st.session_state: st.session_state.origin_df = None
if 'target_df' not in st.session_state: st.session_state.target_df = None
if 'comparison_results' not in st.session_state: st.session_state.comparison_results = None

if 'origin_buckets' not in st.session_state: st.session_state.origin_buckets = []
if 'target_buckets' not in st.session_state: st.session_state.target_buckets = []
if 'origin_bucket_options' not in st.session_state: st.session_state.origin_bucket_options = ["Load buckets first"]
if 'target_bucket_options' not in st.session_state: st.session_state.target_bucket_options = ["Load buckets first"]

if 'origin_tables' not in st.session_state: st.session_state.origin_tables = []
if 'target_tables' not in st.session_state: st.session_state.target_tables = []
if 'origin_table_options' not in st.session_state: st.session_state.origin_table_options = ["Load tables first"]
if 'target_table_options' not in st.session_state: st.session_state.target_table_options = ["Load tables first"]

# DB Type Selection
if 'origin_db_type' not in st.session_state: st.session_state.origin_db_type = "Snowflake"
if 'target_db_type' not in st.session_state: st.session_state.target_db_type = "BigQuery"

# Keboola Connection Inputs - Initialize from secrets
if 'origin_kbc_url_input' not in st.session_state:
    st.session_state.origin_kbc_url_input = st.secrets.get("origin_kbc_url", "https://connection.keboola.com")
if 'origin_token_input' not in st.session_state:
    st.session_state.origin_token_input = st.secrets.get("origin_token", "")
if 'target_kbc_url_input' not in st.session_state:
    st.session_state.target_kbc_url_input = st.secrets.get("target_kbc_url", "https://connection.keboola.com")
if 'target_token_input' not in st.session_state:
    st.session_state.target_token_input = st.secrets.get("target_token", "")

# Origin Snowflake Connection Details - Initialize from secrets
if 'sf_host_origin' not in st.session_state:
    st.session_state.sf_host_origin = st.secrets.get("origin_snowflake", {}).get("host", "")
if 'sf_user_origin' not in st.session_state:
    st.session_state.sf_user_origin = st.secrets.get("origin_snowflake", {}).get("user", "")
if 'sf_password_origin' not in st.session_state:
    st.session_state.sf_password_origin = st.secrets.get("origin_snowflake", {}).get("password", "")
if 'sf_database_origin' not in st.session_state:
    st.session_state.sf_database_origin = st.secrets.get("origin_snowflake", {}).get("database", "")
if 'sf_warehouse_origin' not in st.session_state:
    st.session_state.sf_warehouse_origin = st.secrets.get("origin_snowflake", {}).get("warehouse", "")

# Target Snowflake Connection Details - Initialize from secrets
if 'sf_host_target' not in st.session_state:
    st.session_state.sf_host_target = st.secrets.get("target_snowflake", {}).get("host", "")
if 'sf_user_target' not in st.session_state:
    st.session_state.sf_user_target = st.secrets.get("target_snowflake", {}).get("user", "")
if 'sf_password_target' not in st.session_state:
    st.session_state.sf_password_target = st.secrets.get("target_snowflake", {}).get("password", "")
if 'sf_database_target' not in st.session_state:
    st.session_state.sf_database_target = st.secrets.get("target_snowflake", {}).get("database", "")
if 'sf_warehouse_target' not in st.session_state:
    st.session_state.sf_warehouse_target = st.secrets.get("target_snowflake", {}).get("warehouse", "")

# Origin BigQuery Connection Details - Initialize from secrets
if 'bq_project_id_origin' not in st.session_state:
    st.session_state.bq_project_id_origin = st.secrets.get("origin_bigquery", {}).get("project_id", "")
if 'bq_service_account_json_str_origin' not in st.session_state:
    st.session_state.bq_service_account_json_str_origin = st.secrets.get("origin_bigquery", {}).get("service_account_json_str", "")

# Target BigQuery Connection Details - Initialize from secrets
if 'bq_project_id_target' not in st.session_state:
    st.session_state.bq_project_id_target = st.secrets.get("target_bigquery", {}).get("project_id", "")
if 'bq_service_account_json_str_target' not in st.session_state:
    st.session_state.bq_service_account_json_str_target = st.secrets.get("target_bigquery", {}).get("service_account_json_str", "")

NUMERIC_KEBOOLA_TYPES = ['INTEGER', 'NUMERIC', 'DECIMAL', 'FLOAT', 'REAL', 'DOUBLE PRECISION', 'BIGINT', 'SMALLINT', 'INT', 'NUMBER']

# --- Helper Functions ---
def get_bucket_list(kbc_url: str, token: str) -> list:
    """Connects to Keboola using kbcstorage and returns a list of bucket dicts or an empty list if error."""
    if not token or not kbc_url:
        st.warning("Please enter both KBC URL and Storage API Token.")
        return []
    try:
        client = Client(kbc_url, token)
        buckets = client.buckets.list() # As per sapi-python-client documentation
        return buckets # Returns a list of bucket dictionaries
    except StorageApiError as e:
        # If StorageApiError was successfully imported and is specific, this will catch it.
        st.error(f"Failed to connect to Keboola or list buckets (API Error): {e}")
        return []
    except Exception as e:
        # Catches other errors, including if StorageApiError was aliased to Exception
        st.error(f"An unexpected error occurred: {e}")
        return []

def get_table_list(kbc_url: str, token: str, bucket_id: str) -> list:
    """Connects to Keboola using kbcstorage and returns a list of table dicts for a given bucket_id."""
    if not token or not kbc_url or not bucket_id:
        st.warning("KBC URL, Token, and Bucket ID are required to list tables.")
        return []
    try:
        client = Client(kbc_url, token)
        tables = client.buckets.list_tables(bucket_id=bucket_id)
        return tables # Returns a list of table dictionaries
    except StorageApiError as e:
        st.error(f"Failed to list tables for bucket {bucket_id} (API Error): {e}")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred while listing tables: {e}")
        return []

def get_backend_table_info(kbc_url: str, kbc_token: str, keboola_table_id: str):
    if not all([kbc_url, kbc_token, keboola_table_id]):
        st.error("KBC URL, Token, and Keboola Table ID are required to get backend info.")
        return None
    try:
        client = Client(kbc_url, kbc_token)
        table_details = client.tables.detail(table_id=keboola_table_id)

        if not table_details:
            st.error(f"No details returned for Keboola table {keboola_table_id}.")
            return None
        
        bucket_info = table_details.get('bucket')
        table_logical_name = table_details.get('name')
        columns_list_names = table_details.get('columns')
        column_metadata_dict = table_details.get('columnMetadata')
        if not isinstance(bucket_info, dict):
            st.error(f"Bucket information for table {keboola_table_id} is not structured as expected. Expected a dictionary for 'bucket', got {type(bucket_info)}. Value: {bucket_info}. Full table_details: {table_details}")
            return None

        bucket_id_val = bucket_info.get('id')
        backend_val = bucket_info.get('backend')
        
        if not (bucket_id_val and isinstance(bucket_id_val, str) and
                backend_val and isinstance(backend_val, str) and
                table_logical_name is not None and
                columns_list_names is not None and isinstance(columns_list_names, list) and
                column_metadata_dict is not None and isinstance(column_metadata_dict, dict)):
            st.error(f"Essential information missing, incomplete, or of wrong type in Keboola table details for {keboola_table_id}.\
                       Bucket ID: {bucket_id_val} (type: {type(bucket_id_val)}), Backend: {backend_val} (type: {type(backend_val)}).\
                       Full table_details received: {table_details}")
            return None

        backend_type = backend_val
        schema_name_to_return = None

        if backend_type == 'snowflake':
            schema_name_to_return = bucket_id_val 
        elif backend_type == 'bigquery':
            schema_name_to_return = bucket_id_val.replace('.', '_').replace('-', '_')
        else:
            st.warning(f"Unsupported backend type '{backend_type}' for schema determination for table {keboola_table_id}. Using bucket ID (transformed) as fallback schema.")
            schema_name_to_return = bucket_id_val.replace('.', '_').replace('-', '_') 

        table_name_to_return = table_details.get('tablePhysicalName', table_logical_name)
        
        column_keboola_types = {}
        for col_name in columns_list_names:
            metadata_for_col = column_metadata_dict.get(col_name, [])
            kbc_type = "UNKNOWN"
            for meta_item in metadata_for_col:
                if isinstance(meta_item, dict) and meta_item.get('key') == 'KBC.datatype.basetype':
                    kbc_type = meta_item.get('value', "UNKNOWN")
                    break
            column_keboola_types[col_name] = kbc_type

        return {
            'schema': schema_name_to_return,
            'table': table_name_to_return,
            'columns': columns_list_names,
            'column_keboola_types': column_keboola_types,
            'backend_type': backend_type
        }
    except StorageApiError as e:
        st.error(f"Keboola API Error fetching backend table info for {keboola_table_id}: {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected error fetching backend table info for {keboola_table_id} from Keboola: {e}")
        return None

def get_snowflake_dataframe(sf_host, sf_user, sf_password, sf_database, sf_warehouse, schema_name, table_name, columns_list):
    if not all([sf_host, sf_user, sf_password, sf_database, sf_warehouse, schema_name, table_name, columns_list]):
        st.error("Missing Snowflake connection parameters or table details.")
        return None
    # Ensure column names are quoted to handle special characters and case sensitivity
    quoted_columns = '", "'.join(columns_list)
    query = f'SELECT "{quoted_columns}" FROM "{sf_database}"."{schema_name}"."{table_name}"'
    st.info(f"Executing Snowflake query: {query}")
    try:
        conn = snowflake.connector.connect(user=sf_user, password=sf_password, account=sf_host, warehouse=sf_warehouse, database=sf_database, schema=schema_name)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching data from Snowflake table {sf_database}.{schema_name}.{table_name}: {e}")
        return None

def get_bigquery_dataframe(bq_project_id, bq_service_account_json_str, dataset_name, table_name, columns_list):
    if not all([bq_project_id, bq_service_account_json_str, dataset_name, table_name, columns_list]):
        st.error("Missing BigQuery connection parameters or table details.")
        return None
    
    formatted_columns = ", ".join([f'`{col}`' for col in columns_list])
    query = f'SELECT {formatted_columns} FROM `{bq_project_id}.{dataset_name}.{table_name}`'
    st.info(f"Executing BigQuery query: {query}")
    try:
        credentials_info = json.loads(bq_service_account_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        bq_client = bigquery.Client(project=bq_project_id, credentials=credentials)
        df = bq_client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error fetching data from BigQuery table {bq_project_id}.{dataset_name}.{table_name}: {e}")
        return None

def test_snowflake_connection(host, user, password, database, warehouse):
    if not all([host, user, password, database, warehouse]):
        return False, "Snowflake connection failed: All fields are required."
    try:
        conn = snowflake.connector.connect(
            user=user, password=password, account=host, 
            warehouse=warehouse, database=database
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        return True, "Snowflake connection successful!"
    except Exception as e:
        return False, f"Snowflake connection failed: {e}"

def test_bigquery_connection(project_id, service_account_json_str):
    if not project_id or not service_account_json_str.strip():
        return False, "BigQuery connection failed: GCP Project ID and Service Account JSON are required."
    try:
        credentials_info = json.loads(service_account_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(project=project_id, credentials=credentials)
        client.query("SELECT 1").result()
        return True, "BigQuery connection successful!"
    except json.JSONDecodeError:
        return False, "BigQuery connection failed: Invalid JSON format for service account credentials."
    except Exception as e:
        return False, f"BigQuery connection failed: {e}"

def generate_aggregate_queries(db_type: str, db_name_or_project_id: str, backend_info: dict, attempt_cast: bool, other_table_column_types: dict | None = None) -> tuple[dict, list]:
    """
    Generates SQL queries to calculate aggregates (MIN, MAX, AVG, SUM, COUNT) 
    for numeric columns in a table.
    Optionally attempts to cast the column to a numeric type before aggregation.
    If a column type is UNKNOWN, it attempts to infer it from other_table_column_types.

    Args:
        db_type (str): Type of the database ("Snowflake" or "BigQuery").
        db_name_or_project_id (str): Database name (Snowflake) or Project ID (BigQuery).
        backend_info (dict): Backend information for the current table.
        attempt_cast (bool): Whether to attempt casting to a numeric type.
        other_table_column_types (dict | None): Column types {name: type} of the other table for inference.

    Returns:
        tuple[dict, list]: A tuple containing the dictionary of queries and a list of inference notification messages.
    """
    queries = {}
    inference_messages = []
    if not backend_info or not db_name_or_project_id:
        st.warning(f"Cannot generate aggregate queries: Missing backend_info or database/project identifier for {db_type}.")
        return queries, inference_messages

    schema_name = backend_info.get('schema')
    table_name = backend_info.get('table')
    table_identifier_for_logging = f"{schema_name}.{table_name}" if schema_name and table_name else "current table"
    columns_list = backend_info.get('columns', [])
    column_keboola_types = backend_info.get('column_keboola_types', {})

    if not schema_name or not table_name:
        st.warning(f"Cannot generate aggregate queries: Missing schema or table name in backend_info for {db_type}.")
        return queries, inference_messages
        
    for col_name in columns_list:
        effective_keboola_type = column_keboola_types.get(col_name, "UNKNOWN").upper()
        original_type = effective_keboola_type

        if effective_keboola_type == "UNKNOWN" and other_table_column_types:
            inferred_type_from_other = other_table_column_types.get(col_name, "UNKNOWN").upper()
            if inferred_type_from_other != "UNKNOWN":
                effective_keboola_type = inferred_type_from_other
                message = f"Info: For column '{col_name}' in {table_identifier_for_logging} ({db_type}), data type '{inferred_type_from_other}' was inferred from the other table (original was UNKNOWN)."
                inference_messages.append(message)
                # st.info(message) # Display immediately or collect?

        if effective_keboola_type in NUMERIC_KEBOOLA_TYPES:
            column_expression_for_agg = ""
            raw_column_identifier = ""

            if db_type == "Snowflake":
                raw_column_identifier = f'"{col_name}"'
                if attempt_cast and original_type == "UNKNOWN": # Only attempt cast if original was UNKNOWN and we inferred numeric
                    column_expression_for_agg = f'TRY_CAST({raw_column_identifier} AS NUMBER)'
                elif attempt_cast: # Standard cast attempt
                     column_expression_for_agg = f'TRY_CAST({raw_column_identifier} AS NUMBER)'
                else:
                    column_expression_for_agg = raw_column_identifier
            elif db_type == "BigQuery":
                raw_column_identifier = f'`{col_name}`'
                if attempt_cast and original_type == "UNKNOWN":
                    column_expression_for_agg = f'SAFE_CAST({raw_column_identifier} AS NUMERIC)'
                elif attempt_cast:
                    column_expression_for_agg = f'SAFE_CAST({raw_column_identifier} AS NUMERIC)'
                else:
                    column_expression_for_agg = raw_column_identifier
            else:
                st.warning(f"Unsupported database type '{db_type}' for column expression generation.")
                continue

            min_agg = f'MIN({column_expression_for_agg}) AS min_{col_name}'
            max_agg = f'MAX({column_expression_for_agg}) AS max_{col_name}'
            avg_agg = f'AVG({column_expression_for_agg}) AS avg_{col_name}'
            sum_agg = f'SUM({column_expression_for_agg}) AS sum_{col_name}'
            count_agg = f'COUNT({column_expression_for_agg}) AS count_{col_name}'

            select_clause_aggregates = [min_agg, max_agg, avg_agg, sum_agg, count_agg]
            select_clause = ", ".join(select_clause_aggregates)

            if db_type == "Snowflake":
                full_table_path = f'"{db_name_or_project_id}"."{schema_name}"."{table_name}"'
                queries[col_name] = f"SELECT {select_clause} FROM {full_table_path};"
            elif db_type == "BigQuery":
                full_table_path = f'`{db_name_or_project_id}.{schema_name}.{table_name}`'
                queries[col_name] = f"SELECT {select_clause} FROM {full_table_path};"
    
    if not queries and columns_list: # Only show if there were columns but no numeric ones found/inferred
        st.info(f"No numeric columns identified or inferred for aggregate query generation in table {table_identifier_for_logging} ({db_type}).")
    return queries, inference_messages

def execute_aggregate_query(db_type: str, connection_params: dict, query_string: str):
    """Executes a single aggregate SQL query and returns the result as a dictionary."""
    if not query_string:
        return None
    
    st.info(f"Executing ({db_type}): {query_string}") # Log the query being run
    try:
        if db_type == "Snowflake":
            conn = snowflake.connector.connect(**connection_params)
            cursor = conn.cursor(snowflake.connector.DictCursor) # Fetch as dict
            cursor.execute(query_string)
            result = cursor.fetchone() # Expecting a single row of aggregates
            cursor.close()
            conn.close()
            return result
        elif db_type == "BigQuery":
            credentials_info = json.loads(connection_params['service_account_json_str'])
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            bq_client = bigquery.Client(project=connection_params['project_id'], credentials=credentials)
            query_job = bq_client.query(query_string)
            results_iter = query_job.result() # Waits for the job to complete
            # Convert to list of dicts, should be one row
            rows = [dict(row) for row in results_iter]
            return rows[0] if rows else None
        else:
            st.error(f"Unsupported database type '{db_type}' for query execution.")
            return None
    except Exception as e:
        st.error(f"Error executing {db_type} query \"{query_string}\": {e}")
        return None

def perform_comparison(origin_agg_results: dict, target_agg_results: dict, 
                       origin_backend_info: dict, target_backend_info: dict):
    """Compares aggregate results for numeric columns and returns a structured list.
    Expects origin_agg_results and target_agg_results to be pre-processed (e.g., rounded if necessary).
    """
    comparison_output = []
    
    origin_keboola_types = origin_backend_info.get('column_keboola_types', {})
    target_keboola_types = target_backend_info.get('column_keboola_types', {})

    # Find common columns that have aggregate results in both origin and target
    # Ensure keys exist before forming the set
    origin_keys = set(origin_agg_results.keys()) if origin_agg_results else set()
    target_keys = set(target_agg_results.keys()) if target_agg_results else set()
    common_columns = origin_keys.intersection(target_keys)

    if not common_columns and (origin_keys or target_keys): # Columns exist but none are common
        st.warning("No common columns found with aggregate results to compare.")
        # Optionally, list columns unique to each if helpful
        # unique_to_origin = origin_keys - target_keys
        # unique_to_target = target_keys - origin_keys
        # if unique_to_origin: st.info(f"Columns with aggregates only in Origin: {unique_to_origin}")
        # if unique_to_target: st.info(f"Columns with aggregates only in Target: {unique_to_target}")


    for col_name in common_columns:
        origin_metrics_raw = origin_agg_results.get(col_name, {})
        target_metrics_raw = target_agg_results.get(col_name, {})

        if isinstance(origin_metrics_raw, dict) and origin_metrics_raw.get("error") or \
           isinstance(target_metrics_raw, dict) and target_metrics_raw.get("error"):
            comparison_output.append({
                'column_name': col_name,
                'origin_keboola_type': origin_keboola_types.get(col_name, 'N/A'),
                'target_keboola_type': target_keboola_types.get(col_name, 'N/A'),
                'error': 'Error fetching aggregates for one or both sources.',
                'overall_column_status': 'ERROR'
            })
            continue
        
        # Metrics should already be parsed (e.g. min_COLNAME -> min) and potentially rounded
        # by the calling function (apply_rounding_and_process_comparison)
        parsed_origin_metrics = origin_metrics_raw 
        parsed_target_metrics = target_metrics_raw
                
        metrics_to_compare = ['min', 'max', 'avg', 'sum', 'count']
        metrics_comparison_status = {}
        all_match = True
        
        for metric_key in metrics_to_compare:
            origin_val = parsed_origin_metrics.get(metric_key)
            target_val = parsed_target_metrics.get(metric_key)
            
            # DEBUGGING:
            print(f"Metric: {metric_key} --- Comparing Origin: {origin_val} (Type: {type(origin_val)}) --- Target: {target_val} (Type: {type(target_val)})")
            
            match = False # Default to False
            if origin_val is None and target_val is None:
                match = True
            elif origin_val is None or target_val is None:
                match = False
            elif isinstance(origin_val, (int, float)) and isinstance(target_val, (int, float)):
                try:
                    val1_float = float(origin_val)
                    val2_float = float(target_val)
                    match = abs(val1_float - val2_float) < 1e-9
                except (ValueError, TypeError):
                    match = str(origin_val) == str(target_val) 
            else: 
                match = str(origin_val) == str(target_val)
                
            metrics_comparison_status[f'{metric_key}_match'] = match
            if not match:
                all_match = False

        comparison_output.append({
            'column_name': col_name,
            'origin_keboola_type': origin_keboola_types.get(col_name, 'N/A'),
            'target_keboola_type': target_keboola_types.get(col_name, 'N/A'),
            'origin_metrics': parsed_origin_metrics,
            'target_metrics': parsed_target_metrics,
            'metrics_comparison': metrics_comparison_status,
            'overall_column_status': 'MATCH' if all_match else 'MISMATCH'
        })
        
    return comparison_output

def extract_metrics_from_df(df, column_name, data_type, origin_or_target):
    metrics = {}
    if column_name not in df.columns:
        for agg_func_name in ['min', 'max', 'avg', 'sum', 'count', 'distinct_count', 'zero_count', 'neg_count']:
            metrics[agg_func_name] = "N/A" # Explicitly N/A if column missing
        metrics['null_count'] = df.shape[0]
        return metrics

    try:
        metrics['count'] = int(df[column_name].count())
        metrics['null_count'] = int(df[column_name].isnull().sum())
        metrics['distinct_count'] = int(df[column_name].nunique())
    except Exception as e:
        metrics['count'] = "Error"
        metrics['null_count'] = "Error"
        metrics['distinct_count'] = "Error"

    numeric_column = pd.to_numeric(df[column_name], errors='coerce')

    if not numeric_column.isnull().all(): # If there's at least one valid number
        agg_functions = {
            'min': numeric_column.min,
            'max': numeric_column.max,
            'avg': numeric_column.mean,
            'sum': numeric_column.sum
        }
        for name, func in agg_functions.items():
            try:
                val = func()
                if pd.notna(val):
                    if isinstance(val, float) and val == int(val):
                        metrics[name] = int(val)
                    else:
                        metrics[name] = float(val) # Store as float if has decimal or ensure it's float
                else:
                    metrics[name] = None
            except Exception as e:
                metrics[name] = "Error"
        
        try:
            metrics['zero_count'] = int((numeric_column == 0).sum())
        except Exception as e:
            metrics['zero_count'] = "Error"
        
        try:
            metrics['neg_count'] = int((numeric_column < 0).sum())
        except Exception as e:
            metrics['neg_count'] = "Error"
    else:
        for name in ['min', 'max', 'avg', 'sum', 'zero_count', 'neg_count']:
            metrics[name] = "N/A"
        if data_type not in NUMERIC_KEBOOLA_TYPES and data_type != "UNKNOWN":
            pass # Expected N/A

    # Final cleanup for None, "Error", "N/A" - already handled by setting None or "Error"/"N/A" directly.
    # Ensure all numeric metric keys exist, even if N/A or Error
    for k in ['min', 'max', 'avg', 'sum', 'zero_count', 'neg_count']:
        if k not in metrics: # Should not happen with current logic but as a safeguard
            metrics[k] = "N/A"
            
    return metrics

# Helper function to parse metrics from the stored dictionary
def parse_and_round_metrics(metrics_dict, should_round, round_digits):
    if should_round:
        for key, value in metrics_dict.items():
            if pd.isna(value):
                metrics_dict[key] = None
            elif isinstance(value, float) and value == int(value):
                metrics_dict[key] = int(value)
    return metrics_dict

# --- Callback for rounding changes ---
def handle_rounding_change():
    """Callback function to re-process comparison with new rounding settings."""
    if st.session_state.get('origin_backend_info') and st.session_state.get('target_backend_info'):
        apply_rounding_and_process_comparison(
            perform_comparison_func=perform_comparison, # from app.py
            origin_backend_info=st.session_state.origin_backend_info,
            target_backend_info=st.session_state.target_backend_info
        )
    # else: 
        # If backend info isn't there, it means the main "Check" hasn't successfully run.
        # apply_rounding_and_process_comparison has its own checks for raw data availability.
        # Silently do nothing or provide a very subtle hint if necessary, 
        # but the primary flow relies on "Check" populating necessary states.
        # st.sidebar.info("Run 'Check' to load data before changing rounding.")

# --- Main App Layout ---
st.title("Keboola Project Comparison")

# Render the configuration UI from the separate module
# Pass helper functions as arguments
selected_origin_keboola_table_id_display, selected_target_keboola_table_id_display = render_configuration_ui(
    get_bucket_list_func=get_bucket_list,
    get_table_list_func=get_table_list,
    test_snowflake_connection_func=test_snowflake_connection,
    test_bigquery_connection_func=test_bigquery_connection
)

st.divider()

# --- Comparison Controls and Results Display Area Setup ---
st.divider()
st.header("Comparison Controls & Results")

# "Check" button is primary trigger
st.session_state.attempt_cast_checkbox = st.checkbox("Attempt numeric cast before aggregation? (Uses TRY_CAST/SAFE_CAST)", value=False, key="attempt_cast_cb")
st.session_state.fetch_full_data_debug = st.checkbox("Fetch Full DataFrames (for debugging/viewing - optional)", value=False, key="fetch_full_data_debug_cb")

if st.button("Check", type="primary", key="check_button"):
    st.session_state.check_button_pressed = True
    execute_comparison_workflow(
        selected_origin_keboola_table_id_display,
        selected_target_keboola_table_id_display,
        get_backend_table_info_func=get_backend_table_info,
        generate_aggregate_queries_func=generate_aggregate_queries,
        execute_aggregate_query_func=execute_aggregate_query,
        perform_comparison_func=perform_comparison, # Pass the function from app.py
        get_snowflake_dataframe_func=get_snowflake_dataframe,
        get_bigquery_dataframe_func=get_bigquery_dataframe
    )

# Rounding controls moved AFTER the "Check" button and will use on_change
st.divider() # Visual separation for rounding controls
st.subheader("Display Options")
col_round_check, col_round_digits = st.columns([1,3])
with col_round_check:
    st.checkbox("Round Results?", value=False, key="round_numbers_checkbox", on_change=handle_rounding_change)
with col_round_digits:
    st.number_input("Decimal Places", min_value=0, max_value=10, value=2, step=1, key="rounding_digits_input", on_change=handle_rounding_change, disabled=not st.session_state.get('round_numbers_checkbox', False))

# --- Display sections for results (populated by execute_comparison_workflow via session_state) ---

# Display Type Inference Notifications if any
if st.session_state.get('check_button_pressed', False) and st.session_state.get('type_inference_notifications'):
    st.subheader("Data Type Inference Notifications")
    for msg in st.session_state.type_inference_notifications:
        st.info(msg)
    st.divider()

# Display Backend Information if available
if st.session_state.get('origin_backend_info') or st.session_state.get('target_backend_info'):
    st.subheader("Backend Information & Column Types")
    if st.session_state.get('origin_backend_info'):
        with st.expander("Origin Backend Info", expanded=False):
            st.json(st.session_state.origin_backend_info)
    if st.session_state.get('target_backend_info'):
        with st.expander("Target Backend Info", expanded=False):
            st.json(st.session_state.target_backend_info)

# Display Generated Aggregate Queries if available
if st.session_state.get('origin_agg_queries') or st.session_state.get('target_agg_queries'):
    with st.expander("Generated Aggregate Queries (for numeric columns)", expanded=False):
        if st.session_state.get('origin_agg_queries'):
            st.markdown("##### Origin Table Queries:")
            for col, query in st.session_state.origin_agg_queries.items():
                st.text_area(f"Origin - {col}", query, height=100, disabled=True, key=f"query_origin_{col}_expander")
        elif st.session_state.get('check_button_pressed', False):
            st.info("No aggregate queries generated for the origin table (check for numeric columns and backend info).")

        if st.session_state.get('target_agg_queries'):
            st.markdown("##### Target Table Queries:")
            for col, query in st.session_state.target_agg_queries.items():
                st.text_area(f"Target - {col}", query, height=100, disabled=True, key=f"query_target_{col}_expander")
        elif st.session_state.get('check_button_pressed', False):
            st.info("No aggregate queries generated for the target table (check for numeric columns and backend info).")

# Display Fetched Aggregate Results (Raw) if available
if st.session_state.get('origin_agg_results_raw') or st.session_state.get('target_agg_results_raw'):
    st.subheader("Fetched Aggregate Results (Raw)")
    if st.session_state.get('origin_agg_results_raw'):
        with st.expander("Origin Aggregate Results (Raw JSON)", expanded=False):
            st.json(st.session_state.origin_agg_results_raw)
    if st.session_state.get('target_agg_results_raw'):
        with st.expander("Target Aggregate Results (Raw JSON)", expanded=False):
            st.json(st.session_state.target_agg_results_raw)

# Display Full DataFrames (Optional, if fetched)
if st.session_state.get("fetch_full_data_debug_cb", False): # Check the checkbox state directly
    if st.session_state.get('origin_df') is not None:
        st.subheader("Origin DataFrame Head & Info")
        st.write("Shape:", st.session_state.origin_df.shape)
        st.dataframe(st.session_state.origin_df.head())
    elif st.session_state.get('check_button_pressed', False):
        st.warning("Could not fetch Origin DataFrame (or it was not requested).")

    if st.session_state.get('target_df') is not None:
        st.subheader("Target DataFrame Head & Info")
        st.write("Shape:", st.session_state.target_df.shape)
        st.dataframe(st.session_state.target_df.head())
    elif st.session_state.get('check_button_pressed', False):
        st.warning("Could not fetch Target DataFrame (or it was not requested).")

st.divider()
st.subheader("Detailed Column Comparison")
if st.session_state.get('comparison_results'):
    for col_data in st.session_state.comparison_results:
        status_color = "green" if col_data.get('overall_column_status') == 'MATCH' else "orange" if col_data.get('overall_column_status') == 'MISMATCH' else "red"
        expander_title = f":{status_color}[{col_data.get('overall_column_status', 'UNKNOWN')}] - Column: {col_data['column_name']}"
        
        with st.expander(expander_title, expanded=col_data.get('overall_column_status') != 'MATCH'):
            if col_data.get('error'):
                st.error(col_data['error'])
                continue

            st.markdown(f"**Origin Keboola Type:** `{col_data.get('origin_keboola_type')}` --- **Target Keboola Type:** `{col_data.get('target_keboola_type')}`")
            
            res_col1, res_col2, res_col3 = st.columns(3)
            with res_col1:
                st.markdown("**Metric**")
            with res_col2:
                st.markdown("**Origin Value**")
            with res_col3:
                st.markdown("**Target Value**")
            
            metrics_to_display = ['min', 'max', 'avg', 'sum', 'count']
            for metric in metrics_to_display:
                origin_val = col_data['origin_metrics'].get(metric, 'N/A')
                target_val = col_data['target_metrics'].get(metric, 'N/A')
                match_status = col_data['metrics_comparison'].get(f'{metric}_match', False)
                
                # Values should already be processed (rounded) by comparison_logic.py
                display_origin_val = origin_val
                display_target_val = target_val
                
                res_col1.markdown(metric.upper())
                res_col2.markdown(f"`{display_origin_val}`" + (" ✅" if match_status else " ❌" if display_origin_val != 'N/A' and display_target_val != 'N/A' and not match_status else ""))
                res_col3.markdown(f"`{display_target_val}`" + (" ✅" if match_status else " ❌" if display_origin_val != 'N/A' and display_target_val != 'N/A' and not match_status else ""))
elif st.session_state.get('check_button_pressed', False):
    st.info("No comparison results to display. This might be due to errors fetching aggregates, no common numeric columns, or an issue in the comparison process.")

st.divider()
# The old placeholder UI from the image (Table 1 status: miss etc.) should be removed if this new dynamic display is complete.
# with st.container():
#     st.subheader("Table 1 (status: miss), Matched: 1, Missed: 3")
# ... (rest of old placeholder) 