{{ config(materialized='view', tags=['MergePlIntRatePlMargin']) }}

WITH JoinCatID AS (
	SELECT
		PL_INT_RATE_AMT_INT_RATE_AMT_2,
		PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_2,
		{{ ref('Switch_30') }}.PL_INT_RATE_AMT_PL_INT_RATE_ID AS leftRec_PL_INT_RATE_AMT_PL_INT_RATE_ID,
		PL_INT_RATE_AMT_INT_RATE_AMT_1,
		PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_1,
		{{ ref('Switch_30') }}.PL_INT_RATE_AMT_PL_INT_RATE_ID AS rightRec_PL_INT_RATE_AMT_PL_INT_RATE_ID
	FROM {{ ref('Switch_30') }}
	OUTER JOIN {{ ref('Switch_30') }} ON {{ ref('Switch_30') }}.PL_INT_RATE_AMT_PL_INT_RATE_ID = {{ ref('Switch_30') }}.PL_INT_RATE_AMT_PL_INT_RATE_ID
)

SELECT * FROM JoinCatID