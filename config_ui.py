import streamlit as st

# Note: The helper functions (get_bucket_list, get_table_list, test_snowflake_connection, test_bigquery_connection)
# are expected to be passed as arguments to render_configuration_ui from app.py.

def render_configuration_ui(
    get_bucket_list_func, 
    get_table_list_func, 
    test_snowflake_connection_func, 
    test_bigquery_connection_func
):
    """
    Renders the entire configuration section for the Streamlit app.
    Manages UI elements for Keboola connections, bucket/table selection, 
    and database connection details for both origin and target.

    Args:
        get_bucket_list_func: Function to get list of buckets.
        get_table_list_func: Function to get list of tables in a bucket.
        test_snowflake_connection_func: Function to test Snowflake connection.
        test_bigquery_connection_func: Function to test BigQuery connection.

    Returns:
        tuple: (selected_origin_keboola_table_id_display, selected_target_keboola_table_id_display)
    """
    selected_origin_keboola_table_id_display_local = None
    selected_target_keboola_table_id_display_local = None

    with st.expander("Configuration", expanded=True):
        col_origin, col_target = st.columns(2)

        with col_origin:
            st.subheader("Origin Project Setup")
            st.caption("Keboola details for discovering buckets/tables:")
            origin_kbc_url = st.text_input("Origin KBC URL", key="origin_kbc_url_input", value=st.session_state.origin_kbc_url_input)
            origin_token = st.text_input("Origin Storage API Token", type="password", key="origin_token_input", value=st.session_state.origin_token_input)
            
            if st.button("Load Origin Buckets", key="load_origin_buckets_btn"):
                st.session_state.origin_table_options = ["Load tables first"]
                st.session_state.origin_tables = []
                if origin_token and origin_kbc_url:
                    st.session_state.origin_buckets = get_bucket_list_func(origin_kbc_url, origin_token)
                    st.session_state.origin_bucket_options = [f"{b['id']} ({b['name']})" for b in st.session_state.origin_buckets] if st.session_state.origin_buckets else ["No buckets found or error"]
                else:
                    st.session_state.origin_bucket_options = ["Enter KBC URL and token first"]
                    st.session_state.origin_buckets = []
            
            selected_origin_bucket_display = st.selectbox("Origin Bucket", options=st.session_state.origin_bucket_options, key="origin_bucket_select")
            origin_selected_keboola_bucket_id = selected_origin_bucket_display.split(" (")[0] if selected_origin_bucket_display and selected_origin_bucket_display not in ["Load buckets first", "No buckets found or error", "Enter KBC URL and token first"] else None
            
            if origin_selected_keboola_bucket_id and st.button("Load Tables from Origin Bucket", key="load_origin_tables_btn"):
                if origin_token and origin_kbc_url:
                    st.session_state.origin_tables = get_table_list_func(origin_kbc_url, origin_token, origin_selected_keboola_bucket_id)
                    st.session_state.origin_table_options = [f"{t['id']} ({t['name']})" for t in st.session_state.origin_tables] if st.session_state.origin_tables else ["No tables found or error"]
                else:
                    st.session_state.origin_table_options = ["Missing KBC URL, token, or bucket selection"]
                    st.session_state.origin_tables = []
            
            selected_origin_keboola_table_id_display = st.selectbox("Origin Table", options=st.session_state.origin_table_options, key="origin_table_select")

            st.divider()
            st.caption("Origin Database for Data Comparison:")
            st.session_state.origin_db_type = st.radio(
                "Origin Database Type", 
                ("Snowflake", "BigQuery"),
                key="origin_db_type_radio", 
                horizontal=True
            )

            if st.session_state.origin_db_type == "Snowflake":
                st.markdown("##### Snowflake Connection Details (Origin)")
                st.session_state.sf_host_origin = st.text_input("Snowflake Host/Account", value=st.session_state.sf_host_origin, key="sf_host_origin_input_exp_fix")
                st.session_state.sf_user_origin = st.text_input("Snowflake User", value=st.session_state.sf_user_origin, key="sf_user_origin_input_exp_fix")
                st.session_state.sf_password_origin = st.text_input("Snowflake Password", type="password", value=st.session_state.sf_password_origin, key="sf_password_origin_input_exp_fix")
                st.session_state.sf_database_origin = st.text_input("Snowflake Database", value=st.session_state.sf_database_origin, key="sf_db_origin_input_exp_fix")
                st.session_state.sf_warehouse_origin = st.text_input("Snowflake Warehouse", value=st.session_state.sf_warehouse_origin, key="sf_wh_origin_input_exp_fix")
                if st.button("Test Origin DB Connection", key="test_origin_db_btn_sf"):
                    success, message = test_snowflake_connection_func(st.session_state.sf_host_origin, st.session_state.sf_user_origin, st.session_state.sf_password_origin, st.session_state.sf_database_origin, st.session_state.sf_warehouse_origin)
                    if success: st.success(message)
                    else: st.error(message)
            elif st.session_state.origin_db_type == "BigQuery":
                st.markdown("##### BigQuery Connection Details (Origin)")
                st.session_state.bq_project_id_origin = st.text_input("GCP Project ID (Origin)", value=st.session_state.bq_project_id_origin, key="bq_project_id_origin_input_exp_fix")
                st.session_state.bq_service_account_json_str_origin = st.text_area("BigQuery Service Account JSON (Origin)", value=st.session_state.bq_service_account_json_str_origin, key="bq_json_origin_input_exp_fix", height=150)
                if st.button("Test Origin DB Connection", key="test_origin_db_btn_bq"):
                    success, message = test_bigquery_connection_func(st.session_state.bq_project_id_origin, st.session_state.bq_service_account_json_str_origin)
                    if success: st.success(message)
                    else: st.error(message)

        with col_target:
            st.subheader("Target Project Setup")
            st.caption("Keboola details for discovering buckets/tables:")
            target_kbc_url = st.text_input("Target KBC URL", key="target_kbc_url_input", value=st.session_state.target_kbc_url_input)
            target_token = st.text_input("Target Storage API Token", type="password", key="target_token_input", value=st.session_state.target_token_input)
            
            if st.button("Load Target Buckets", key="load_target_buckets_btn"):
                st.session_state.target_table_options = ["Load tables first"]
                st.session_state.target_tables = []
                if target_token and target_kbc_url:
                    st.session_state.target_buckets = get_bucket_list_func(target_kbc_url, target_token)
                    st.session_state.target_bucket_options = [f"{b['id']} ({b['name']})" for b in st.session_state.target_buckets] if st.session_state.target_buckets else ["No buckets found or error"]
                else:
                    st.session_state.target_bucket_options = ["Enter KBC URL and token first"]
                    st.session_state.target_buckets = []
            
            selected_target_bucket_display = st.selectbox("Target Bucket", options=st.session_state.target_bucket_options, key="target_bucket_select")
            target_selected_keboola_bucket_id = selected_target_bucket_display.split(" (")[0] if selected_target_bucket_display and selected_target_bucket_display not in ["Load buckets first", "No buckets found or error", "Enter KBC URL and token first"] else None
            
            if target_selected_keboola_bucket_id and st.button("Load Tables from Target Bucket", key="load_target_tables_btn"):
                if target_token and target_kbc_url:
                    st.session_state.target_tables = get_table_list_func(target_kbc_url, target_token, target_selected_keboola_bucket_id)
                    st.session_state.target_table_options = [f"{t['id']} ({t['name']})" for t in st.session_state.target_tables] if st.session_state.target_tables else ["No tables found or error"]
                else:
                    st.session_state.target_table_options = ["Missing KBC URL, token, or bucket selection"]
                    st.session_state.target_tables = []
            
            selected_target_keboola_table_id_display = st.selectbox("Target Table", options=st.session_state.target_table_options, key="target_table_select")
            
            st.divider()
            st.caption("Target Database for Data Comparison:")
            st.session_state.target_db_type = st.radio(
                "Target Database Type", 
                ("Snowflake", "BigQuery"),
                key="target_db_type_radio", 
                horizontal=True,
                index=st.session_state.target_db_type_radio_index if 'target_db_type_radio_index' in st.session_state else 1
            )
            if st.session_state.target_db_type == "Snowflake": st.session_state.target_db_type_radio_index = 0
            elif st.session_state.target_db_type == "BigQuery": st.session_state.target_db_type_radio_index = 1

            if st.session_state.target_db_type == "Snowflake":
                st.markdown("##### Snowflake Connection Details (Target)")
                st.session_state.sf_host_target = st.text_input("Snowflake Host/Account", value=st.session_state.sf_host_target, key="sf_host_target_input_exp_fix")
                st.session_state.sf_user_target = st.text_input("Snowflake User", value=st.session_state.sf_user_target, key="sf_user_target_input_exp_fix")
                st.session_state.sf_password_target = st.text_input("Snowflake Password", type="password", value=st.session_state.sf_password_target, key="sf_password_target_input_exp_fix")
                st.session_state.sf_database_target = st.text_input("Snowflake Database", value=st.session_state.sf_database_target, key="sf_db_target_input_exp_fix")
                st.session_state.sf_warehouse_target = st.text_input("Snowflake Warehouse", value=st.session_state.sf_warehouse_target, key="sf_wh_target_input_exp_fix")
                if st.button("Test Target DB Connection", key="test_target_db_btn_sf"):
                    success, message = test_snowflake_connection_func(st.session_state.sf_host_target, st.session_state.sf_user_target, st.session_state.sf_password_target, st.session_state.sf_database_target, st.session_state.sf_warehouse_target)
                    if success: st.success(message)
                    else: st.error(message)
            elif st.session_state.target_db_type == "BigQuery":
                st.markdown("##### BigQuery Connection Details (Target)")
                st.session_state.bq_project_id_target = st.text_input("GCP Project ID (Target)", value=st.session_state.bq_project_id_target, key="bq_project_id_target_input_exp_fix")
                st.session_state.bq_service_account_json_str_target = st.text_area("BigQuery Service Account JSON (Target)", value=st.session_state.bq_service_account_json_str_target, key="bq_json_target_input_exp_fix", height=150)
                if st.button("Test Target DB Connection", key="test_target_db_btn_bq"):
                    success, message = test_bigquery_connection_func(st.session_state.bq_project_id_target, st.session_state.bq_service_account_json_str_target)
                    if success: st.success(message)
                    else: st.error(message)
    
    return selected_origin_keboola_table_id_display, selected_target_keboola_table_id_display 