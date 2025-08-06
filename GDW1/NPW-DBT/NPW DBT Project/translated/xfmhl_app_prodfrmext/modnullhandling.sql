{{ config(materialized='view', tags=['XfmHL_APP_PRODFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PAYT_FREQ_C: string[max=1]= handle_null (PAYT_FREQ_C, '9')
	--TARG_CHAR_C: string[max=255]= handle_null (TARG_CHAR_C, '999007')
	HL_APP_PROD_ID, PARENT_HL_APP_PROD_ID, HL_REPAYMENT_PERIOD_CAT_ID, AMOUNT, LOAN_TERM_MONTHS, ACCOUNT_NUMBER, TOTAL_LOAN_AMOUNT, HLS_FLAG, GDW_UPDATED_LDP_PAID_ON_AMOUNT, SRCE_CHAR_1_C, ORIG_ETL_D, PAYT_FREQ_C, TARG_CHAR_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling