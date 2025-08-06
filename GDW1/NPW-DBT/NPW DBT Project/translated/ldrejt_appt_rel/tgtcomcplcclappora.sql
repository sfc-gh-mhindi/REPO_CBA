{{ config(materialized='view', tags=['LdREJT_APPT_REL']) }}

SELECT
	APPT_I
	RELD_APPT_I
	APPT_QLFY_C
	LOAN_APPT_QLFY_C
	ETL_D
	ORIG_ETL_D
	EROR_C 
FROM {{ ref('ModConversions') }}