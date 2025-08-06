{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH FunMergeJournal AS (
	SELECT
		LOAN_SUBTYPE_CODE as LOAN_SUBTYPE_CODE,
		APPT_I as APPT_I,
		REL_TYPE_C as REL_TYPE_C,
		RELD_APPT_I as RELD_APPT_I,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		LOAN_SUBTYPE_CODE,
		APPT_I,
		REL_TYPE_C,
		RELD_APPT_I,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal