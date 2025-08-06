{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH FunMergeJournal AS (
	SELECT
		HL_APP_PROD_PURPOSE_ID as HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID as HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID as HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT as AMOUNT,
		MAIN_PURPOSE as MAIN_PURPOSE,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal