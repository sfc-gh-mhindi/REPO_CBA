{{ config(materialized='view', tags=['LdREJT_APPT_QSTN']) }}

SELECT
	APP_ID
	SUBTYPE_CODE
	QA_QUESTION_ID
	QA_ANSWER_ID
	TEXT_ANSWER
	CIF_CODE
	ETL_D
	ORIG_ETL_D
	EROR_C 
FROM {{ ref('ModConversions') }}