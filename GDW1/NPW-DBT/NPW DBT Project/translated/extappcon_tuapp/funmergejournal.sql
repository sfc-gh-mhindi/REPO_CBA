{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH FunMergeJournal AS (
	SELECT
		SUBTYPE_CODE as SUBTYPE_CODE,
		HL_APP_PROD_ID as HL_APP_PROD_ID,
		TU_APP_CONDITION_ID as TU_APP_CONDITION_ID,
		TU_APP_CONDITION_CAT_ID as TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_DATE as CONDITION_MET_DATE,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		TU_APP_CONDITION_ID,
		TU_APP_CONDITION_CAT_ID,
		CONDITION_MET_DATE,
		SUBTYPE_CODE,
		HL_APP_PROD_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal