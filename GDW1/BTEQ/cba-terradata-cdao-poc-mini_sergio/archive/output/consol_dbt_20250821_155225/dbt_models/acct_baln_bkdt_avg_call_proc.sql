{%- set process_name = 'ACCT_BALN_BKDT_AVG_CALL' -%}
{%- set stream_name = 'ACCOUNT_BALANCE' -%}

{{
  config(
    materialized='table',
    database=var('target_database'),
    schema='account_balance',
    tags=['account', 'balance', 'monthly', 'average'],
    pre_hook=[
        "{{ log_dcf_exec_msg('Monthly Average Balance calculation started') }}"
    ],
    post_hook=[
        "{{ log_dcf_exec_msg('Monthly Average Balance calculation completed') }}"
    ]
  )
}}

/*
    Model: acct_baln_bkdt_avg_call
    Purpose: Process to calculate the Monthly Average balance sourcing from ACCT BALN BKDT
    Business Logic: Calls stored procedure to calculate average daily balance for previous month
    Dependencies: SP_CALC_AVRG_DAY_BALN_BKDT stored procedure
    
    Original BTEQ Logic:
    - Calls SP_CALC_AVRG_DAY_BALN_BKDT with previous month date parameter
    - Date formatted as YYYYMMDD for previous month from current date
*/

WITH procedure_call AS (
    SELECT 
        CALL {{ var('cad_prod_macro_schema') }}.SP_CALC_AVRG_DAY_BALN_BKDT(
            TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYYMMDD')
        ) AS result
),

final AS (
    SELECT 
        result,
        CURRENT_TIMESTAMP() AS process_timestamp,
        TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'YYYYMMDD') AS process_date_param
    FROM procedure_call
)

SELECT * FROM final