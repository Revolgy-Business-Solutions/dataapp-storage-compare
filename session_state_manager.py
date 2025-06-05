import streamlit as st

def initialize_session_state():
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
    if 'origin_db_type' not in st.session_state: st.session_state.origin_db_type = "Snowflake" # Default or load from secrets
    if 'target_db_type' not in st.session_state: st.session_state.target_db_type = "BigQuery"  # Default or load from secrets

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

    # Comparison Controls Session State
    if 'round_numbers_checkbox' not in st.session_state: st.session_state.round_numbers_checkbox = True
    if 'rounding_digits_input' not in st.session_state: st.session_state.rounding_digits_input = 2
    if 'attempt_cast_checkbox' not in st.session_state: st.session_state.attempt_cast_checkbox = False
    
    # For aggregate queries and results (initialized properly before use in check button logic)
    if 'origin_agg_queries' not in st.session_state: st.session_state.origin_agg_queries = None
    if 'target_agg_queries' not in st.session_state: st.session_state.target_agg_queries = None
    if 'origin_agg_results' not in st.session_state: st.session_state.origin_agg_results = {} # Init as dict
    if 'target_agg_results' not in st.session_state: st.session_state.target_agg_results = {} # Init as dict 

    # For type inference notifications
    if 'type_inference_notifications' not in st.session_state: st.session_state.type_inference_notifications = [] 