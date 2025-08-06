{{ config(materialized='view', tags=['MergePlIntRatePlMargin']) }}

WITH JoinPlIntRateMargin AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_ID,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_DOC_SEQ_NO,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_CASS_MARGIN_AMT,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_INT_RATE_TERM,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_INT_RATE_FREQUENCY_ID,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_VARIANT_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_USAGE_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_PL_APP_PROD_ID,
		{{ ref('CgAdd_Flag1') }}.PL_INT_RATE_FOUND_FLAG,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_MARGIN_PL_MARGIN_ID,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_MARGIN_MARGIN_AMT,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_MARGIN_PL_FEE_ID,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_INT_RATE_ID AS PL_MARGIN_PL_INT_RATE_ID,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_MARGIN_MARGIN_REASON_CAT_ID,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_MARGIN_PL_APP_PROD_ID,
		{{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_MARGIN_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	OUTER JOIN {{ ref('FL_IgnoreNullPlIntRateIdRecs') }} ON {{ ref('CgAdd_Flag1') }}.PL_INT_RATE_ID = {{ ref('FL_IgnoreNullPlIntRateIdRecs') }}.PL_INT_RATE_ID
)

SELECT * FROM JoinPlIntRateMargin