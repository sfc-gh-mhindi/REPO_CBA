{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH FunMergeJournal AS (
	SELECT
		SM_CASE_ID as SM_CASE_ID,
		CREATED_TIMESTAMP as CREATED_TIMESTAMP,
		WIM_PROCESS_ID as WIM_PROCESS_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal