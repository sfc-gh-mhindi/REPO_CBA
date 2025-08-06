{{ config(materialized='view', tags=['LdREJT_APPT_QSTN']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy-%mm-%dd] (ORIG_ETL_D)
	APP_ID, SUBTYPE_CODE, QA_QUESTION_ID, QA_ANSWER_ID, TEXT_ANSWER, CIF_CODE, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunRejectsNulls') }}
)

SELECT * FROM ModConversions