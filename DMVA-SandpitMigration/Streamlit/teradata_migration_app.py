#!/usr/bin/env python3
"""
Teradata to Snowflake Migration Streamlit App
==============================================

A user-friendly interface for executing the P_MIGRATE_TERADATA_TABLE stored procedure
with comprehensive parameter validation, progress monitoring, and result display.

Author: Migration Team
Created: 2025
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import time
import json
from datetime import datetime, timedelta
import re
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

# --- Streamlit Page Configuration and Title ---
st.set_page_config(
    page_title="Teradata Migration Tool",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("Teradata to Snowflake Migration Tool")
st.markdown("**Automated migration with optional chunking optimization**")

# --- Global Plotly Configuration to Avoid WebGL Issues ---
# Set default renderer to avoid WebGL issues in browsers
pio.renderers.default = "svg"

# Global config for all Plotly charts to prevent WebGL errors
PLOTLY_CONFIG = {
    'displayModeBar': False,
    'staticPlot': False,
    'toImageButtonOptions': {'format': 'svg'},
    'plotlyServerURL': None
}

# --- Formatting Dictionary ---
format_dict = {
    "DOLLAR_0_DEC": "${:,.0f}",
    "DOLLAR_2_DEC": "${:,.2f}",
    "WHOLE_NBR": "{:,.0f}",
    "PERCENTAGE": "{:.2f}%"
}

# --- Snowflake Connection (Streamlit-in-Snowflake) ---
@st.cache_resource
def get_snowflake_session():
    """Establishes and caches the active Snowpark session."""
    return get_active_session()

# Get the session object once at the start
session = get_snowflake_session()

def init_session_state():
    """Initialize session state variables"""
    if 'migration_history' not in st.session_state:
        st.session_state.migration_history = []
    if 'last_result' not in st.session_state:
        st.session_state.last_result = None
    if 'last_migration_info' not in st.session_state:
        st.session_state.last_migration_info = None
    if 'data_loaded_at_least_once' not in st.session_state:
        st.session_state.data_loaded_at_least_once = False

def run_snowflake_query(query_str, user_friendly_context=""):
    """Executes a SQL query using the Snowpark session and returns result with user-friendly error handling."""
    try:
        result_df = session.sql(query_str).collect()
        if result_df:
            return result_df[0][0], None  # Return first column of first row
        return "No result returned", None
    except Exception as e:
        error_message = str(e).lower()
        
        # Provide user-friendly error messages based on common error patterns
        if "does not exist" in error_message or "object does not exist" in error_message:
            return None, "📊 **Procedure Not Found**: The migration procedure is not available. Please contact your administrator."
        elif "permission" in error_message or "access" in error_message or "unauthorized" in error_message:
            return None, "🔒 **Access Denied**: You don't have permission to execute the migration procedure. Please contact your administrator."
        elif "timeout" in error_message or "cancelled" in error_message:
            return None, "⏱️ **Query Timeout**: The migration is taking too long. This may be normal for large tables."
        elif "network" in error_message or "connection" in error_message:
            return None, "🌐 **Connection Issue**: Lost connection to Snowflake. Please refresh the page and try again."
        elif "invalid" in error_message and "parameter" in error_message:
            return None, "⚠️ **Invalid Parameters**: Please check your parameter values and try again."
        elif "temporary table" in error_message or "unsupported statement type" in error_message:
            return None, "🚫 **Streamlit Limitation**: This procedure uses temporary tables which are not supported in Streamlit apps. Please run this procedure directly in Snowflake SQL worksheets."
        else:
            return None, f"❌ **Execution Error**: {str(e)}"

def validate_parameters(source_db, source_table, target_db, target_schema, target_table, 
                       chunking_enabled, chunking_column, chunking_value, chunking_type):
    """Validate migration parameters with detailed error messages"""
    errors = []
    
    # Required parameters
    if not source_db.strip():
        errors.append("Source Database Name is required")
    if not source_table.strip():
        errors.append("Source Table Name is required")
    if not target_db.strip():
        errors.append("Target Database Name is required")
    if not target_schema.strip():
        errors.append("Target Schema Name is required")
    if not target_table.strip():
        errors.append("Target Table Name is required")
    
    # Chunking parameters validation
    if chunking_enabled:
        if not chunking_column.strip():
            errors.append("Chunking Column is required when chunking is enabled")
        if not chunking_value.strip():
            errors.append("Chunking Value is required when chunking is enabled")
        if chunking_type not in ['by_integer', 'by_date', 'by_substr']:
            errors.append("Invalid chunking data type selected")
        
        # Validate chunking value based on chunking type
        if chunking_type == 'by_integer' or chunking_type == 'by_substr':
            try:
                int(chunking_value)
            except ValueError:
                errors.append("Chunking Value must be numeric")
        elif chunking_type == 'by_date':
            if chunking_value not in ['day', 'month']:
                errors.append("Chunking Value for by_date must be 'day' or 'month'")
    
    return errors

def generate_migration_sql(source_db, source_table, target_db, target_schema, target_table,
                          chunking_enabled, chunking_column, chunking_value, chunking_type):
    """Generate the SQL statement for the migration procedure"""
    if chunking_enabled:
        sql = f"""
CALL npd_d12_dmn_gdwmig.migration_tracking.P_MIGRATE_TERADATA_TABLE(
    '{source_db}', 
    '{source_table}', 
    '{target_db}', 
    '{target_schema}', 
    '{target_table}',
    'Y', 
    '{chunking_column}', 
    '{chunking_value}', 
    '{chunking_type}'
);
        """
    else:
        sql = f"""
CALL npd_d12_dmn_gdwmig.migration_tracking.P_MIGRATE_TERADATA_TABLE(
    '{source_db}', 
    '{source_table}', 
    '{target_db}', 
    '{target_schema}', 
    '{target_table}'
);
        """
    return sql.strip()

def execute_migration(source_db, source_table, target_db, target_schema, target_table,
                     chunking_enabled, chunking_column, chunking_value, chunking_type):
    """Execute the migration procedure with proper error handling"""
    try:
        sql = generate_migration_sql(source_db, source_table, target_db, target_schema, target_table,
                                   chunking_enabled, chunking_column, chunking_value, chunking_type)
        
        result, error = run_snowflake_query(sql, "executing migration procedure")
        return result, error, sql
        
    except Exception as e:
        error_message = str(e).lower()
        
        # Provide user-friendly error messages
        if "does not exist" in error_message:
            return None, "📊 **Procedure Not Found**: The migration procedure is not available. Please contact your administrator.", None
        elif "permission" in error_message or "access" in error_message:
            return None, "🔒 **Access Denied**: You don't have permission to execute the migration procedure.", None
        elif "timeout" in error_message:
            return None, "⏱️ **Migration Timeout**: The migration is taking longer than expected. This may be normal for large tables.", None
        elif "parameter" in error_message:
            return None, "⚠️ **Parameter Error**: Invalid parameter values provided. Please check your configuration.", None
        elif "temporary table" in error_message or "unsupported statement" in error_message:
            return None, "🚫 **Streamlit Limitation**: This procedure uses features not supported in Streamlit apps. Please run directly in Snowflake.", None
        else:
            return None, f"❌ **Execution Error**: {str(e)}", None

def parse_migration_result(result_text):
    """Parse migration result to extract key information"""
    if not result_text:
        return {}
    
    info = {}
    lines = result_text.split('\n')
    
    for line in lines:
        if 'Source:' in line:
            info['source'] = line.split('Source:')[1].strip()
        elif 'Target:' in line:
            info['target'] = line.split('Target:')[1].strip()
        elif 'Chunking Enabled:' in line:
            info['chunking'] = line.split('Chunking Enabled:')[1].strip()
        elif 'Target table has' in line and 'rows' in line:
            match = re.search(r'(\d+) rows', line)
            if match:
                info['existing_rows'] = int(match.group(1))
        elif 'Step 8: Migration procedure completed!' in line:
            info['status'] = 'Completed'
        elif 'ERROR:' in line:
            info['status'] = 'Error'
            info['error'] = line
        elif 'All migration tasks completed successfully!' in line:
            info['status'] = 'Success'
    
    return info

def main():
    """Main Streamlit application"""
    init_session_state()
    
    # Connection status and warnings in main content
    col_status1, col_status2 = st.columns([2, 1])
    with col_status1:
        st.success("🟢 **Connected to Snowflake** - Using active session")
    # with col_status2:
    #     st.warning("⚠️ **Note**: Some procedures may need manual execution due to Streamlit limitations")
    
    # Initialize session state for migration parameters
    if 'current_source_db' not in st.session_state:
        st.session_state.current_source_db = "B_D52_D_TMP_001_STD_0"
    if 'current_source_table' not in st.session_state:
        st.session_state.current_source_table = "SANDPIT_DEMO_TBL001"
    if 'current_target_db' not in st.session_state:
        st.session_state.current_target_db = "NPD_D12_DMN_GDWMIG_IBRG_V"
    if 'current_target_schema' not in st.session_state:
        st.session_state.current_target_schema = "P_V_STG_001_STD_0"
    if 'current_target_table' not in st.session_state:
        st.session_state.current_target_table = "SANDPIT_DEMO_TBL001"
    if 'current_chunking_enabled' not in st.session_state:
        st.session_state.current_chunking_enabled = False
    if 'current_chunking_column' not in st.session_state:
        st.session_state.current_chunking_column = "CODE1"
    if 'current_chunking_value' not in st.session_state:
        st.session_state.current_chunking_value = "1000000"
    if 'current_chunking_type' not in st.session_state:
        st.session_state.current_chunking_type = "by_integer"
    if 'migration_in_progress' not in st.session_state:
        st.session_state.migration_in_progress = False
    
    # --- Main Content ---
    st.markdown("---")
    st.subheader("📋 Migration Parameters")
    
    with st.form("migration_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🎯 Source Configuration**")
            source_db = st.text_input("Source Database Name", 
                                      value=st.session_state.current_source_db,
                                      help="Teradata schema/database name",
                                      disabled=st.session_state.migration_in_progress)
            source_table = st.text_input("Source Table Name", 
                                        value=st.session_state.current_source_table,
                                        help="Table name in Teradata",
                                        disabled=st.session_state.migration_in_progress)
        
        with col2:
            st.markdown("**🎯 Target Configuration**")
            target_db = st.text_input("Target Database Name", 
                                      value=st.session_state.current_target_db,
                                      help="Snowflake database name",
                                      disabled=st.session_state.migration_in_progress)
            target_schema = st.text_input("Target Schema Name", 
                                         value=st.session_state.current_target_schema,
                                         help="Snowflake schema name",
                                         disabled=st.session_state.migration_in_progress)
            target_table = st.text_input("Target Table Name", 
                                        value=st.session_state.current_target_table,
                                        help="Target table name in Snowflake",
                                        disabled=st.session_state.migration_in_progress)
        
        # Chunking Configuration
        st.markdown("**⚡ Chunking Configuration**")
        chunking_enabled = st.checkbox("Enable Chunking Optimization", 
                                       value=st.session_state.current_chunking_enabled,
                                       help="Recommended for large tables (millions of rows)",
                                       disabled=st.session_state.migration_in_progress)
        
        if chunking_enabled:
            col3, col4, col5 = st.columns(3)
            with col3:
                chunking_column = st.text_input("Chunking Column", 
                                               value=st.session_state.current_chunking_column,
                                               help="Column to use for data chunking",
                                               disabled=st.session_state.migration_in_progress)
            with col4:
                chunking_type = st.selectbox("Chunking Data Type", 
                                            ["by_integer", "by_date", "by_substr"],
                                            index=["by_integer", "by_date", "by_substr"].index(st.session_state.current_chunking_type),
                                            help="Type of chunking method",
                                            disabled=st.session_state.migration_in_progress)
            with col5:
                # Dynamic label and input based on chunking type
                if chunking_type == "by_integer":
                    chunking_value = st.text_input("Chunking Value (Integer Value)", 
                                                  value=st.session_state.current_chunking_value,
                                                  help="Modulus value for integer chunking",
                                                  disabled=st.session_state.migration_in_progress)
                elif chunking_type == "by_substr":
                    chunking_value = st.text_input("Chunking Value (Nbr of Characters)", 
                                                  value=st.session_state.current_chunking_value,
                                                  help="Number of characters for substring chunking",
                                                  disabled=st.session_state.migration_in_progress)
                elif chunking_type == "by_date":
                    # For date type, use dropdown with day/month options
                    date_options = ["day", "month"]
                    current_date_value = st.session_state.current_chunking_value if st.session_state.current_chunking_value in date_options else "day"
                    chunking_value = st.selectbox("Chunking Value (Period)", 
                                                 options=date_options,
                                                 index=date_options.index(current_date_value),
                                                 help="Time period for date chunking",
                                                 disabled=st.session_state.migration_in_progress)
            
            # Chunking explanation
            chunking_info = {
                "by_integer": "Integer column chunking using modulus operation",
                "by_date": "Date column chunking by time period (days or months)",
                "by_substr": "String column chunking by substring length"
            }
            st.info(f"ℹ️ **{chunking_type}**: {chunking_info[chunking_type]}")
        else:
            chunking_column = chunking_value = chunking_type = ""
        
        # Submit buttons
        col_execute, col_generate = st.columns([1, 1])
        with col_execute:
            migration_submitted = st.form_submit_button("🚀 Execute Migration", 
                                                       help="Run the migration procedure directly",
                                                       disabled=st.session_state.migration_in_progress)
        with col_generate:
            generate_sql_submitted = st.form_submit_button("📝 Generate SQL", 
                                                          help="Generate SQL for manual execution",
                                                          disabled=st.session_state.migration_in_progress)
    
    # Handle form submissions
    if migration_submitted or generate_sql_submitted:
        # Set migration in progress for Execute Migration
        if migration_submitted:
            st.session_state.migration_in_progress = True
        
        # Update session state with current values
        st.session_state.current_source_db = source_db
        st.session_state.current_source_table = source_table
        st.session_state.current_target_db = target_db
        st.session_state.current_target_schema = target_schema
        st.session_state.current_target_table = target_table
        st.session_state.current_chunking_enabled = chunking_enabled
        st.session_state.current_chunking_column = chunking_column
        st.session_state.current_chunking_value = chunking_value
        st.session_state.current_chunking_type = chunking_type
        
        # Parameter Validation
        validation_errors = validate_parameters(source_db, source_table, target_db, target_schema, 
                                              target_table, chunking_enabled, chunking_column, 
                                              chunking_value, chunking_type)
        
        if validation_errors:
            st.error("❌ **Parameter Validation Errors:**")
            for error in validation_errors:
                st.write(f"• {error}")
        else:
            # Migration Configuration Summary
            st.markdown("---")
            st.subheader("📊 Migration Configuration")
            
            config_col1, config_col2 = st.columns(2)
            with config_col1:
                st.markdown(f"**📊 Source:** `{source_db}.{source_table}`")
                st.markdown(f"**⚡ Chunking:** `{'Enabled' if chunking_enabled else 'Disabled'}`")
            with config_col2:
                st.markdown(f"**🎯 Target:** `{target_db}.{target_schema}.{target_table}`")
                if chunking_enabled:
                    st.markdown(f"**🔧 Chunking Method:** `{chunking_type} ({chunking_column})`")
            
            # Generate SQL for display
            generated_sql = generate_migration_sql(source_db, source_table, target_db, target_schema, target_table,
                                                 chunking_enabled, chunking_column, chunking_value, chunking_type)
            
            if generate_sql_submitted:
                # Show generated SQL
                st.markdown("---")
                st.subheader("📝 Generated SQL")
                st.info("💡 **Alternative Execution**: If the Streamlit app encounters limitations, copy and run this SQL directly in a Snowflake worksheet.")
                st.code(generated_sql, language="sql")
                
                # Add copy button functionality
                st.markdown("**Instructions for Manual Execution:**")
                st.markdown("""
                1. **Copy the SQL above**
                2. **Open a Snowflake worksheet** in your Snowflake console
                3. **Paste and execute** the SQL statement
                4. **Monitor the results** in the worksheet
                """)
            
            elif migration_submitted:
                # Execute Migration
                with st.spinner("🔄 Executing migration procedure..."):
                    result, error, sql_executed = execute_migration(
                        source_db, source_table, target_db, target_schema, target_table,
                        chunking_enabled, chunking_column, chunking_value, chunking_type
                    )
                    
                    if result:
                        st.session_state.last_result = result
                        migration_info = parse_migration_result(result)
                        
                        # Store migration info in session state for results display
                        migration_info_enriched = {
                            **migration_info,
                            'source_db': source_db,
                            'source_table': source_table,
                            'target_db': target_db,
                            'target_schema': target_schema,
                            'target_table': target_table,
                            'chunking_enabled': chunking_enabled,
                            'chunking_column': chunking_column if chunking_enabled else None,
                            'chunking_type': chunking_type if chunking_enabled else None,
                            'chunking_value': chunking_value if chunking_enabled else None
                        }
                        st.session_state.last_migration_info = migration_info_enriched
                        
                        # Add to history
                        history_entry = {
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'source': f"{source_db}.{source_table}",
                            'target': f"{target_db}.{target_schema}.{target_table}",
                            'chunking': chunking_enabled,
                            'status': migration_info.get('status', 'Unknown'),
                            'result': result
                        }
                        st.session_state.migration_history.append(history_entry)
                        
                        # Display result based on status
                        if 'ERROR:' in result:
                            st.error("❌ **Migration Failed**")
                            st.error("Please check the migration log below for details.")
                        elif migration_info.get('status') in ['Completed', 'Success']:
                            st.success("✅ **Migration Completed Successfully**")
                            st.success("The migration procedure has finished. Please verify your data.")
                        else:
                            st.info("ℹ️ **Migration Status**: Please check the detailed results below.")
                        
                        # Reset migration in progress flag after completion
                        st.session_state.migration_in_progress = False
                    else:
                        st.error(error)
                        # Reset migration in progress flag after error
                        st.session_state.migration_in_progress = False
                        # Clear migration info on error
                        st.session_state.last_migration_info = None
                        
                        # Show alternative execution option
                        if "streamlit limitation" in error.lower() or "temporary table" in error.lower():
                            st.markdown("---")
                            st.subheader("🔧 Alternative Execution Method")
                            st.info("💡 **Workaround**: The procedure can be executed directly in a Snowflake worksheet. Use the SQL below:")
                            st.code(generated_sql, language="sql")
                            
                            with st.expander("📋 Manual Execution Instructions", expanded=True):
                                st.markdown("""
                                **Steps to execute manually:**
                                1. **Copy the SQL statement** shown above
                                2. **Open Snowflake Web UI** and navigate to Worksheets
                                3. **Create a new worksheet** or use an existing one
                                4. **Paste the SQL** into the worksheet
                                5. **Execute the statement** and monitor the results
                                6. **The procedure will run** with full functionality including temporary tables
                                
                                **Why this happens:**
                                - Streamlit apps in Snowflake have certain limitations
                                - Stored procedures using temporary tables cannot be executed from Streamlit
                                - Direct execution in worksheets bypasses these limitations
                                """)
    
    # Add reset button if migration was in progress
    if st.session_state.migration_in_progress:
        st.markdown("---")
        if st.button("🔄 Start New Migration", help="Reset the form to configure a new migration"):
            st.session_state.migration_in_progress = False
            st.rerun()
    
    # --- Results Display Section ---
    if st.session_state.last_result:
        st.markdown("---")
        st.subheader("📊 Migration Results")
        
        # Create tabs for different views
        tab1, tab2 = st.tabs(["📈 Summary", "📋 Detailed Log"])
        
        with tab1:
            if st.session_state.last_migration_info:
                info = st.session_state.last_migration_info
                
                # Key metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Duration", info.get('duration', 'N/A'))
                with col2:
                    st.metric("Status", info.get('status', 'Unknown'))
                with col3:
                    st.metric("Source Rows", f"{info.get('source_count', 'N/A'):,}" if info.get('source_count') and str(info.get('source_count')).isdigit() else info.get('source_count', 'N/A'))
                with col4:
                    st.metric("Target Rows", f"{info.get('target_count', 'N/A'):,}" if info.get('target_count') and str(info.get('target_count')).isdigit() else info.get('target_count', 'N/A'))
                
                # Configuration details
                with st.expander("🔧 Migration Configuration", expanded=False):
                    config_col1, config_col2 = st.columns(2)
                    with config_col1:
                        st.markdown(f"**Source:** `{info.get('source_db', 'N/A')}.{info.get('source_table', 'N/A')}`")
                        st.markdown(f"**Chunking:** {'✅ Enabled' if info.get('chunking_enabled') else '❌ Disabled'}")
                    with config_col2:
                        st.markdown(f"**Target:** `{info.get('target_db', 'N/A')}.{info.get('target_schema', 'N/A')}.{info.get('target_table', 'N/A')}`")
                        if info.get('chunking_enabled'):
                            st.markdown(f"**Chunking Details:** {info.get('chunking_type', 'N/A')} on `{info.get('chunking_column', 'N/A')}`")
        
        with tab2:
            st.code(st.session_state.last_result, language="text")
    
    # --- Target Table Data Viewer Section ---
    st.markdown("---")
    st.subheader("📊 Target Table Data Viewer")
    
    # Show data button - disabled during migration
    show_data_clicked = st.button("🔍 Show Data in Target Table", 
                                  help="Display top 1000 rows from the target table",
                                  disabled=st.session_state.migration_in_progress)
    
    if show_data_clicked:
        try:
            # Get current target table configuration
            current_target_db = st.session_state.current_target_db
            current_target_schema = st.session_state.current_target_schema  
            current_target_table = st.session_state.current_target_table
            
            if not all([current_target_db, current_target_schema, current_target_table]):
                st.error("❌ Please configure the target database, schema, and table name first.")
            else:
                with st.spinner(f"Loading data from {current_target_db}.{current_target_schema}.{current_target_table}..."):
                    # Create the query to select top 1000 rows
                    query = f"SELECT * FROM {current_target_db}.{current_target_schema}.{current_target_table} LIMIT 1000"
                    
                    # Execute the query
                    session = get_snowflake_session()
                    if session:
                        result_df = session.sql(query).to_pandas()
                        
                        if not result_df.empty:
                            st.success(f"✅ Found {len(result_df):,} rows (showing top 1000)")
                            
                            # Display data info
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Total Rows Displayed", f"{len(result_df):,}")
                            with col2:
                                st.metric("Total Columns", len(result_df.columns))
                            
                            # Display the data grid
                            st.dataframe(result_df, 
                                       use_container_width=True,
                                       height=400)
                            
                            # Show column info
                            with st.expander("📋 Column Information", expanded=False):
                                col_info = []
                                for col in result_df.columns:
                                    dtype = str(result_df[col].dtype)
                                    null_count = result_df[col].isnull().sum()
                                    col_info.append({
                                        "Column": col,
                                        "Data Type": dtype,
                                        "Null Count": null_count,
                                        "Non-Null Count": len(result_df) - null_count
                                    })
                                
                                st.dataframe(col_info, use_container_width=True)
                        else:
                            st.warning(f"⚠️ The table {current_target_db}.{current_target_schema}.{current_target_table} exists but contains no data.")
                    else:
                        st.error("❌ Unable to connect to Snowflake. Please check your session.")
                        
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "object does not exist" in error_msg:
                st.error(f"❌ Table {current_target_db}.{current_target_schema}.{current_target_table} does not exist.")
                st.info("💡 Run the migration first to create the target table.")
            elif "access denied" in error_msg or "insufficient privileges" in error_msg:
                st.error(f"❌ Access denied to table {current_target_db}.{current_target_schema}.{current_target_table}.")
                st.info("💡 Please check your permissions for the target database and schema.")
            else:
                st.error(f"❌ Error loading data: {str(e)}")
                st.info("💡 Please verify the target table configuration and try again.")

if __name__ == "__main__":
    main() 