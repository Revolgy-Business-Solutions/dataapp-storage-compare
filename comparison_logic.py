import streamlit as st
import json # For BQ credentials if passed as string and parsed here
import pandas as pd
import numpy as np
import copy
# Import helper functions if they are not passed as arguments
# from app import get_backend_table_info, generate_aggregate_queries, ... (adjust as per actual passing mechanism)

def apply_rounding_and_process_comparison(
    perform_comparison_func, 
    origin_backend_info, 
    target_backend_info
):
    """
    Retrieves raw aggregate results from session state, applies key parsing and rounding 
    if specified, and then calls the main comparison function.
    Stores the final comparison results in st.session_state.comparison_results.
    """
    if 'origin_agg_results_raw' not in st.session_state or \
       'target_agg_results_raw' not in st.session_state:
        # This can happen if the "Check" button hasn't been pressed yet
        # or if raw results weren't stored properly.
        # Silently return or display a minimal message if needed, 
        # as the main "Check" flow handles initial population.
        if st.session_state.get('check_button_pressed', False): # only warn if check was pressed
             st.warning("Raw aggregate data not available in session state for rounding/re-comparison. Please run the 'Check' first.")
        st.session_state.comparison_results = []
        return

    raw_origin_column_aggregates = st.session_state.origin_agg_results_raw
    raw_target_column_aggregates = st.session_state.target_agg_results_raw
    
    processed_origin_results = {}
    processed_target_results = {}

    should_round = st.session_state.get('round_numbers_checkbox', False)
    round_digits = st.session_state.get('rounding_digits_input', 3)

    # Process Origin Results
    for col_name, raw_metrics_dict in raw_origin_column_aggregates.items():
        if not isinstance(raw_metrics_dict, dict): # Handle error strings or other non-dict data
            processed_origin_results[col_name] = raw_metrics_dict 
            continue
        
        parsed_and_processed_metrics = {}
        for raw_metric_key, value in raw_metrics_dict.items():
            # E.g., min_COLNAME -> min
            parsed_key = raw_metric_key.split(f'_{col_name}')[0].lower() if f'_{col_name}' in raw_metric_key else raw_metric_key.lower()
            
            val_to_process = value
            if should_round and isinstance(val_to_process, (float, np.floating, int, np.integer)):
                try:
                    # Ensure conversion to float before rounding, especially for np types
                    float_val = float(val_to_process)
                    rounded_val = round(float_val, round_digits)
                    if rounded_val == int(rounded_val): # Check if it's a whole number
                        val_to_process = int(rounded_val)
                    else:
                        val_to_process = rounded_val
                except (ValueError, TypeError):
                    pass # Keep original if not convertible/roundable
            elif isinstance(val_to_process, (float, np.floating)) and val_to_process == int(val_to_process):
                # If not rounding, but it's a float that is a whole number, convert to int for consistency
                 val_to_process = int(val_to_process)

            parsed_and_processed_metrics[parsed_key] = val_to_process
        processed_origin_results[col_name] = parsed_and_processed_metrics

    # Process Target Results
    for col_name, raw_metrics_dict in raw_target_column_aggregates.items():
        if not isinstance(raw_metrics_dict, dict): # Handle error strings
            processed_target_results[col_name] = raw_metrics_dict
            continue

        parsed_and_processed_metrics = {}
        for raw_metric_key, value in raw_metrics_dict.items():
            parsed_key = raw_metric_key.split(f'_{col_name}')[0].lower() if f'_{col_name}' in raw_metric_key else raw_metric_key.lower()
            
            val_to_process = value
            if should_round and isinstance(val_to_process, (float, np.floating, int, np.integer)):
                try:
                    float_val = float(val_to_process)
                    rounded_val = round(float_val, round_digits)
                    if rounded_val == int(rounded_val):
                        val_to_process = int(rounded_val)
                    else:
                        val_to_process = rounded_val
                except (ValueError, TypeError):
                    pass
            elif isinstance(val_to_process, (float, np.floating)) and val_to_process == int(val_to_process):
                 val_to_process = int(val_to_process)

            parsed_and_processed_metrics[parsed_key] = val_to_process
        processed_target_results[col_name] = parsed_and_processed_metrics
        
    st.session_state.comparison_results = perform_comparison_func(
        origin_agg_results=processed_origin_results,
        target_agg_results=processed_target_results,
        origin_backend_info=origin_backend_info, # Passed through
        target_backend_info=target_backend_info  # Passed through
    )

def execute_comparison_workflow(
    selected_origin_keboola_table_id_display: str,
    selected_target_keboola_table_id_display: str,
    get_backend_table_info_func,
    generate_aggregate_queries_func,
    execute_aggregate_query_func,
    perform_comparison_func,
    get_snowflake_dataframe_func,
    get_bigquery_dataframe_func
):
    """
    Manages the full comparison workflow when the 'Check' button is pressed.
    Fetches backend info, generates and executes aggregate queries, performs comparison,
    and updates st.session_state with all results.

    Args:
        selected_origin_keboola_table_id_display (str): Display string for the selected origin Keboola table.
        selected_target_keboola_table_id_display (str): Display string for the selected target Keboola table.
        get_backend_table_info_func (callable): Function to get backend table info.
        generate_aggregate_queries_func (callable): Function to generate aggregate queries.
        execute_aggregate_query_func (callable): Function to execute a single aggregate query.
        perform_comparison_func (callable): Function to compare aggregate results.
        get_snowflake_dataframe_func (callable): Function to fetch a full DataFrame from Snowflake.
        get_bigquery_dataframe_func (callable): Function to fetch a full DataFrame from BigQuery.
    """

    # Reset relevant session state variables at the beginning of the flow
    st.session_state.origin_df = None
    st.session_state.target_df = None
    st.session_state.comparison_results = None
    st.session_state.origin_agg_queries = None
    st.session_state.target_agg_queries = None
    st.session_state.origin_agg_results_raw = {} # Store raw results here
    st.session_state.target_agg_results_raw = {} # Store raw results here
    st.session_state.origin_backend_info = None
    st.session_state.target_backend_info = None
    st.session_state.type_inference_notifications = [] # Initialize for new messages

    origin_keboola_table_id = selected_origin_keboola_table_id_display.split(" (")[0] if selected_origin_keboola_table_id_display and selected_origin_keboola_table_id_display not in ["Load tables first", "No tables found or error"] else None
    target_keboola_table_id = selected_target_keboola_table_id_display.split(" (")[0] if selected_target_keboola_table_id_display and selected_target_keboola_table_id_display not in ["Load tables first", "No tables found or error"] else None

    if not origin_keboola_table_id or not target_keboola_table_id:
        st.error("Please select both an origin and a target table from Keboola for comparison.")
        return

    st.info(f"Starting comparison for Origin: {origin_keboola_table_id} and Target: {target_keboola_table_id}")

    # Fetch Keboola connection details from session state
    origin_kbc_url_val = st.session_state.get('origin_kbc_url_input', st.secrets.get("origin_kbc_url", ""))
    origin_token_val = st.session_state.get('origin_token_input', st.secrets.get("origin_token", ""))
    target_kbc_url_val = st.session_state.get('target_kbc_url_input', st.secrets.get("target_kbc_url", ""))
    target_token_val = st.session_state.get('target_token_input', st.secrets.get("target_token", ""))

    # 1. Get Backend Table Info
    st.session_state.origin_backend_info = get_backend_table_info_func(origin_kbc_url_val, origin_token_val, origin_keboola_table_id)
    st.session_state.target_backend_info = get_backend_table_info_func(target_kbc_url_val, target_token_val, target_keboola_table_id)

    if not st.session_state.origin_backend_info or not st.session_state.target_backend_info:
        st.error("Could not retrieve backend table information for one or both tables. Comparison aborted.")
        # UI will show the detailed error from get_backend_table_info_func
        return

    # Prepare column types for inference
    origin_column_types = st.session_state.origin_backend_info.get('column_keboola_types', {})
    target_column_types = st.session_state.target_backend_info.get('column_keboola_types', {})

    # 2. Generate Aggregate Queries
    origin_db_identifier = st.session_state.sf_database_origin if st.session_state.origin_db_type == "Snowflake" else st.session_state.bq_project_id_origin
    st.session_state.origin_agg_queries, origin_inference_msgs = generate_aggregate_queries_func(
        st.session_state.origin_db_type,
        origin_db_identifier,
        st.session_state.origin_backend_info,
        st.session_state.attempt_cast_checkbox,
        other_table_column_types=target_column_types # Pass target types for inference
    )
    st.session_state.type_inference_notifications.extend(origin_inference_msgs)

    target_db_identifier = st.session_state.sf_database_target if st.session_state.target_db_type == "Snowflake" else st.session_state.bq_project_id_target
    st.session_state.target_agg_queries, target_inference_msgs = generate_aggregate_queries_func(
        st.session_state.target_db_type,
        target_db_identifier,
        st.session_state.target_backend_info,
        st.session_state.attempt_cast_checkbox,
        other_table_column_types=origin_column_types # Pass origin types for inference
    )
    st.session_state.type_inference_notifications.extend(target_inference_msgs)

    # 3. Execute Aggregate Queries
    # Origin
    if st.session_state.origin_agg_queries:
        origin_conn_params = {}
        if st.session_state.origin_db_type == "Snowflake":
            origin_conn_params = {
                'user': st.session_state.sf_user_origin, 'password': st.session_state.sf_password_origin,
                'account': st.session_state.sf_host_origin, 'warehouse': st.session_state.sf_warehouse_origin,
                'database': st.session_state.sf_database_origin
            }
        elif st.session_state.origin_db_type == "BigQuery":
            origin_conn_params = {
                'project_id': st.session_state.bq_project_id_origin,
                'service_account_json_str': st.session_state.bq_service_account_json_str_origin
            }
        
        for col_name, query_str in st.session_state.origin_agg_queries.items():
            result = execute_aggregate_query_func(st.session_state.origin_db_type, origin_conn_params, query_str)
            st.session_state.origin_agg_results_raw[col_name] = result if result else {"error": f"Failed to fetch aggregates for {col_name}"}
    
    # Target
    if st.session_state.target_agg_queries:
        target_conn_params = {}
        if st.session_state.target_db_type == "Snowflake":
            target_conn_params = {
                'user': st.session_state.sf_user_target, 'password': st.session_state.sf_password_target,
                'account': st.session_state.sf_host_target, 'warehouse': st.session_state.sf_warehouse_target,
                'database': st.session_state.sf_database_target
            }
        elif st.session_state.target_db_type == "BigQuery":
            target_conn_params = {
                'project_id': st.session_state.bq_project_id_target,
                'service_account_json_str': st.session_state.bq_service_account_json_str_target
            }

        for col_name, query_str in st.session_state.target_agg_queries.items():
            result = execute_aggregate_query_func(st.session_state.target_db_type, target_conn_params, query_str)
            st.session_state.target_agg_results_raw[col_name] = result if result else {"error": f"Failed to fetch aggregates for {col_name}"}

    # 4. Perform Comparison
    if st.session_state.origin_agg_results_raw and st.session_state.target_agg_results_raw:
        apply_rounding_and_process_comparison(
            perform_comparison_func=perform_comparison_func,
            origin_backend_info=st.session_state.origin_backend_info,
            target_backend_info=st.session_state.target_backend_info
        )
    elif st.session_state.origin_agg_queries or st.session_state.target_agg_queries : # if queries were generated but results are missing
        st.warning("Could not perform comparison as aggregate results for one or both sources are missing or incomplete.")
        st.session_state.comparison_results = []
    else: # No queries generated, likely no numeric columns or backend info issues earlier
        # No specific warning here as earlier stages would have shown info/errors
        st.session_state.comparison_results = []


    # 5. Optional: Fetch Full DataFrames (if checkbox is selected in UI)
    # The checkbox state st.session_state.fetch_full_data_debug is read directly from UI in app.py
    # This logic could also be passed or checked here if preferred.
    # However, to make this module self-contained for the "Check" button's core logic,
    # it's better to include it here based on a session_state flag set by the UI.

    if st.session_state.get("fetch_full_data_debug_cb", False): # Checkbox in app.py sets this via its key
        origin_schema = st.session_state.origin_backend_info.get('schema')
        origin_table = st.session_state.origin_backend_info.get('table')
        origin_cols_names = st.session_state.origin_backend_info.get('columns')

        target_schema = st.session_state.target_backend_info.get('schema')
        target_table = st.session_state.target_backend_info.get('table')
        target_cols_names = st.session_state.target_backend_info.get('columns')

        if st.session_state.origin_db_type == "Snowflake":
            st.session_state.origin_df = get_snowflake_dataframe_func(
                st.session_state.sf_host_origin, st.session_state.sf_user_origin,
                st.session_state.sf_password_origin, st.session_state.sf_database_origin,
                st.session_state.sf_warehouse_origin, origin_schema, origin_table, origin_cols_names
            )
        elif st.session_state.origin_db_type == "BigQuery":
            st.session_state.origin_df = get_bigquery_dataframe_func(
                st.session_state.bq_project_id_origin,
                st.session_state.bq_service_account_json_str_origin,
                origin_schema, origin_table, origin_cols_names
            )
        
        if st.session_state.target_db_type == "Snowflake":
            st.session_state.target_df = get_snowflake_dataframe_func(
                st.session_state.sf_host_target, st.session_state.sf_user_target,
                st.session_state.sf_password_target, st.session_state.sf_database_target,
                st.session_state.sf_warehouse_target, target_schema, target_table, target_cols_names
            )
        elif st.session_state.target_db_type == "BigQuery":
            st.session_state.target_df = get_bigquery_dataframe_func(
                st.session_state.bq_project_id_target,
                st.session_state.bq_service_account_json_str_target,
                target_schema, target_table, target_cols_names
            )
    st.success("Comparison workflow executed.")
    # Results are in st.session_state for app.py to render. 