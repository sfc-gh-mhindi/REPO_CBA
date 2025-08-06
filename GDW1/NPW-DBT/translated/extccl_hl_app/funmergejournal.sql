{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH FunMergeJournal AS (
	SELECT
		CCL_HL_APP_ID as CCL_HL_APP_ID,
		CCL_APP_ID as CCL_APP_ID,
		HL_APP_ID as HL_APP_ID,
		LMI_AMT as LMI_AMT,
		HL_PACKAGE_CAT_ID as HL_PACKAGE_CAT_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal