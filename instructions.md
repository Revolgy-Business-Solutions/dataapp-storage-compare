## Context
I want to create a streamlit application which is responsible for comparing 2 keboola projects. I want to compare content of tables. I made a migration from Snowflake to BiGQuery backend and I want to make sure that I migrated data correctly.

## Documentation
Use following documentations:
https://github.com/keboola/sapi-python-client

## User Inputs
 - origin KBC URL
 - storage token of origin project
 - target KBC URL
 - storage token of target project
 - origin bucket id (selected from list)
 - target bucket id (selected from list)
 - origin table id (selected from list)
 - target table id (selected from list)
 - user can select if origin/target project is bigQuery or Snowflake. It can be selected via radio button in target/origin project. 
 - Snowflake connection (all necesssary things like hostname, username, pass, database, warehouse). This should be foldable panel. Add "Test connnection button"
  - BigQuery Connection. This should be foldable panel. Add "Test connection" button. Credentials.json can be inserted via textarea.
  - Test connection button tries to run SELECT 1 (or similar) on target databasee


 ## UX
 UX is described in file BQ-migration-app-Data Migration.png
 Buttons to interact with user:
  - load all buckets
  - after selecting a bucket there will be a button to list all tables in bucket

 ## Steps
  - [x] Create a dummy UX without any functionality
  - [x] Implement basic logic to connect Keboola project and list all the bucket
  - [x] Implement data comparision logic

## Comparison logic
We want to compare 2 selected tables. For numeric columns we want to calculate min, max, average. And compare results within both tables. This should happen when user clicks "check" button. Run queries againts Snowflake and bigQuery project. Origin project is running Snowflake and target project is running BigQuery. If one of columns has datatype defined use that data type. E.g. when origin table has no data types defined but target one has. Then use target's one. 


## How To
 - always create just one steps and than run the app
 - when step is complete always make sure to check checkbox in this file
 - when I change definition by chat. always make sure to update this file as well
 - [x] Always create/update diagram showing how application works. Save diagram in separate mmd file. 

 ## Technical notes
  - connection to BigQuery should be done via credentials.json
  - full path to Snowflake table should be like "KBC_EUW3_1649"."out.c-price-per-package-calculation"."price_per_package"
  - full path to BigQuery table should be like `kbc-euw3-1733-9190`.`in_c_price_per_package_calculation`. All "-" a converted to "_"
  - all logical parts of app should be in a separate files. E.g. 1 file for configuration, 1 file for rendering the results, 1 file to hold everything around session_state.

## Security crendetials
 - [x] create .streamlit/secrets.toml where I can provide default for all inputs

## Debugging
 - [x] Display extra debugging information when the "Check" button is pressed and relevant data is available.
 - Debug informations displayed (within collapsible `st.expander` sections):
   - [x] Backend Information & Column Types: Detailed metadata for origin and target tables (from `get_backend_table_info` function, stored in `st.session_state.origin_backend_info` and `st.session_state.target_backend_info`).
   - [x] Generated Aggregate Queries: SQL queries generated for numeric columns (from `generate_aggregate_queries` function, stored in `st.session_state.origin_agg_queries` and `st.session_state.target_agg_queries`).
   - [x] Fetched Aggregate Results (Raw): Raw JSON results from the executed aggregate queries (from `execute_aggregate_query` function, stored in `st.session_state.origin_agg_results` and `st.session_state.target_agg_results`).
 - [x] Add a checkbox "Fetch Full DataFrames (for debugging/viewing - optional)": Allows users to retrieve and view the head of the origin and target tables (DataFrames stored in `st.session_state.origin_df` and `st.session_state.target_df`).
 - Consider adding a direct download option for table metadata in the future if requested.
