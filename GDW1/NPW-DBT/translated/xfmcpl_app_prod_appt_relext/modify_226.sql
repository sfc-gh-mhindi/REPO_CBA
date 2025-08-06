{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH Modify_226 AS (
	SELECT 
	--Manual
	--APPT_QLFY_C: string[max=2]= handle_null (APPT_QLFY_C, '99')
	--LOAN_APPT_QLFY_C: string[max=2]= handle_null (LOAN_APPT_QLFY_C, '99')
	APPT_I, RELD_APPT_I, ORIG_ETL_D, APPT_QLFY_C, LOAN_APPT_QLFY_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM Modify_226