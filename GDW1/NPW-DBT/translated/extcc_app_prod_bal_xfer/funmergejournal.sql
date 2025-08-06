{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH FunMergeJournal AS (
	SELECT
		CC_APP_PROD_BAL_XFER_ID as CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID as BAL_XFER_OPTION_CAT_ID,
		XFER_AMT as XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID as BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID as CC_APP_PROD_ID,
		CC_APP_ID as CC_APP_ID,
		ORIG_ETL_D as ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec') }}
	UNION ALL
	SELECT
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID,
		ORIG_ETL_D
	FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec') }}
)

SELECT * FROM FunMergeJournal