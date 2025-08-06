{{ config(materialized='view', tags=['MergePlIntRatePlMargin']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_ID AS PL_INT_RATE_PL_INT_RATE_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_DOC_SEQ_NO,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_CASS_MARGIN_AMT,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_INT_RATE_TERM,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_INT_RATE_FREQUENCY_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_VARIANT_CAT_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_USAGE_CAT_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_PL_APP_PROD_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_FOUND_FLAG,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_PL_MARGIN_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_MARGIN_AMT,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_PL_FEE_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_PL_INT_RATE_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_MARGIN_REASON_CAT_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_PL_APP_PROD_ID,
		{{ ref('JoinPlIntRateMargin') }}.PL_MARGIN_FOUND_FLAG,
		{{ ref('CgAdd_Flag3') }}.PL_INT_RATE_AMT_INT_RATE_AMT_2,
		{{ ref('CgAdd_Flag3') }}.PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_2,
		{{ ref('CgAdd_Flag3') }}.PL_INT_RATE_ID AS PL_INT_RATE_AMT_PL_INT_RATE_ID,
		{{ ref('CgAdd_Flag3') }}.PL_INT_RATE_AMT_INT_RATE_AMT_1,
		{{ ref('CgAdd_Flag3') }}.PL_INT_RATE_AMT_PL_INT_RATE_CAT_ID_1,
		{{ ref('CgAdd_Flag3') }}.PL_INT_RATE_AMT_FOUND_FLAG
	FROM {{ ref('JoinPlIntRateMargin') }}
	OUTER JOIN {{ ref('CgAdd_Flag3') }} ON {{ ref('JoinPlIntRateMargin') }}.PL_INT_RATE_ID = {{ ref('CgAdd_Flag3') }}.PL_INT_RATE_ID
)

SELECT * FROM JoinAll