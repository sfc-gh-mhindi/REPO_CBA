{{ config(materialized='view', tags=['MergePlFeePlMargin']) }}

WITH JoinPlFeeMargin AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.PL_FEE_ID AS PL_FEE_PL_FEE_ID,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_ADD_TO_TOTAL_FLAG,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_FEE_AMT,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_BASE_FEE_AMT,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_PAY_FEE_AT_FUNDING_FLAG,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_START_DATE,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_PL_CAPITALIS_FEE_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_FEE_SCREEN_DESC,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_FEE_DESC,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_CASS_FEE_CODE,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_CASS_FEE_KEY,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_TOTAL_FEE_AMT,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_PL_APP_PROD_ID,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_MARGIN_PL_MARGIN_ID,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_MARGIN_MARGIN_AMT,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_FEE_ID AS PL_MARGIN_PL_FEE_ID,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_MARGIN_PL_INT_RATE_ID,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_MARGIN_MARGIN_REASON_CAT_ID,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_MARGIN_PL_APP_PROD_ID,
		{{ ref('CgAdd_Flag1') }}.PL_FEE_FOUND_FLAG,
		{{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_MARGIN_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	OUTER JOIN {{ ref('FL_IgnoreNullPlFeeIdRecs') }} ON {{ ref('CgAdd_Flag1') }}.PL_FEE_ID = {{ ref('FL_IgnoreNullPlFeeIdRecs') }}.PL_FEE_ID
)

SELECT * FROM JoinPlFeeMargin