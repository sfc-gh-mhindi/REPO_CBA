{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH Modify_226 AS (
	SELECT 
	--Manual
	--APPT_QLFY_C: string[max=2]= handle_null (APPT_QLFY_C, '99')
	--ROW_S: string[max=1]= handle_null (ROW_S, '9')
	--QA_ANSWER_ID: string[max=12]= handle_null (QA_ANSWER_ID, '9')
	LOAN_SBTY_CODE AS APP_ID, APPT_I AS APPT_QLFY_C, RELD_APPT_I AS QSTN_ID, ORIG_ETL_D AS QA_ANSWER_ID, APPT_QLFY_C AS TEXT_ANSWER, CIF_CODE, ROW_S 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM Modify_226