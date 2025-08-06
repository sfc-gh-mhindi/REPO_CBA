{{ config(materialized='view', tags=['LdREJT_APPT_REL']) }}

WITH FunRejectsNulls AS (
	SELECT
		APPT_I as APPT_I,
		RELD_APPT_I as RELD_APPT_I,
		REL_TYPE_C as REL_TYPE_C,
		LOAN_SUBTYPE_CODE as LOAN_SUBTYPE_CODE,
		ETL_D as ETL_D,
		ORIG_ETL_D as ORIG_ETL_D,
		EROR_C as EROR_C
	FROM {{ ref('SrcIdNullsDS') }}
	UNION ALL
	SELECT
		APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		LOAN_SUBTYPE_CODE,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM {{ ref('SrcAppCclAppRejectsDS') }}
)

SELECT * FROM FunRejectsNulls