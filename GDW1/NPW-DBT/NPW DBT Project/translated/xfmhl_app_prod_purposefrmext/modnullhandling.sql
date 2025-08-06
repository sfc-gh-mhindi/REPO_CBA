{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PURP_TYPE_C: string[max=5]= handle_null (PURP_TYPE_C, '9999')
	HL_APP_PROD_PURPOSE_ID, HL_APP_PROD_ID, HL_LOAN_PURPOSE_CAT_ID, AMOUNT, MAIN_PURPOSE, ORIG_ETL_D, PURP_C AS PURP_TYPE_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling