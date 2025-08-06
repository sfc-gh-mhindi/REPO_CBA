{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH FunMergeJournal AS (
	SELECT
		PL_APP_ID as PL_APP_ID,
		NOMINATED_BRANCH_ID as NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID as PL_PACKAGE_CAT_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		PL_APP_ID,
		NOMINATED_BRANCH_ID,
		PL_PACKAGE_CAT_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal