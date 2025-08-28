/*
  GDW1 Account Balance Monthly Average Calculation
  
  Migrated from: ACCT_BALN_BKDT_AVG_CALL_PROC.sql
  
  Description: 
  Process to calculate the Monthly Average balance sourcing from ACCT_BALN_BKDT.
  Calls the stored procedure SP_CALC_AVRG_DAY_BALN_BKDT with the previous month's date.
  
  Original Logic:
  - Call stored procedure with previous month's date
  - Date formatted as YYYYMMDD
  - Processing triggers monthly balance calculations
  
  DBT Approach:
  - Use post-hook to execute the stored procedure
  - Create a minimal model for process tracking
  - Maintain DCF logging for procedure execution
  
  DCF Pattern: Process control operation
  Stream: GDW1_ACCT_BALN_PROCESSING
*/

{{ 
  config(
    materialized='table',
    tags=['intermediate', 'gdw1_migration', 'account_balance', 'procedure_call'],
    pre_hook=[
      "{{ register_gdw1_process_instance(this.name, var('account_balance_stream')) }}",
      "{{ log_gdw1_exec_msg(this.name, var('account_balance_stream'), 10, 'Starting monthly average balance calculation procedure') }}"
    ],
    post_hook=[
      "CALL {{ bteq_var('CAD_PROD_MACRO') }}.SP_CALC_AVRG_DAY_BALN_BKDT('" ~ add_months('CURRENT_DATE()', -1) ~ "')",
      "{{ log_gdw1_exec_msg(this.name, var('account_balance_stream'), 10, 'Monthly average balance calculation completed') }}",
      "{{ update_gdw1_process_status(this.name, var('account_balance_stream'), 'COMPLETED', 'Procedure executed successfully') }}"
    ]
  )
}}

-- Tracking table for procedure execution
SELECT
    'SP_CALC_AVRG_DAY_BALN_BKDT' AS PROCEDURE_NAME,
    {{ add_months('CURRENT_DATE()', -1) }} AS CALCULATION_DATE,
    'MONTHLY_AVERAGE_BALANCE' AS CALCULATION_TYPE,
    'ACCT_BALN_BKDT' AS SOURCE_TABLE,
    
    -- Process tracking information
    '{{ var("account_balance_stream") }}' AS STREAM_NAME,
    {{ generate_process_key() }} AS PROCESS_KEY,
    
    -- Add DCF audit columns
    {{ gdw1_audit_columns() }} 