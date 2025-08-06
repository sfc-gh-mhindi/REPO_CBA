{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH FunMergeSmCaseState AS (
	SELECT
		SM_CASE_STATE_ID as SM_CASE_STATE_ID,
		SM_CASE_ID as SM_CASE_ID,
		SM_STATE_CAT_ID as SM_STATE_CAT_ID,
		START_DATE as START_DATE,
		END_DATE as END_DATE,
		CREATED_BY_STAFF_NUMBER as CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID as STATE_CAUSED_BY_ACTION_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeSmCaseState