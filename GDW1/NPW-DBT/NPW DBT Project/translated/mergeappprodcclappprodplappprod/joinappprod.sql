{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH JoinAppProd AS (
	SELECT
		{{ ref('CgAdd_Flag1') }}.APP_PROD_ID AS CPL_APP_PROD_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_PL_TARGET_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_REPAY_APPROX_AMT,
		{{ ref('CgAdd_Flag1') }}.CPL_REPAY_FREQUENCY_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_PL_APP_PROD_REPAY_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_PL_PROD_TERM_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_PL_CAMPAIGN_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_AD_HOC_CAMPAIGN_DESC,
		{{ ref('CgAdd_Flag1') }}.CPL_CAR_SEEKER_FLAG,
		{{ ref('CgAdd_Flag1') }}.CPL_PL_PROD_TARGET_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_PL_MARKETING_SOURCE_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_HLS_ACCT_NO,
		{{ ref('CgAdd_Flag1') }}.CPL_TOTAL_INTEREST_AMT,
		{{ ref('CgAdd_Flag1') }}.CPL_APP_PROD_AMT,
		{{ ref('CgAdd_Flag1') }}.CPL_TP_BROKER_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_TP_BROKER_FIRST_NAME,
		{{ ref('CgAdd_Flag1') }}.CPL_TP_BROKER_LAST_NAME,
		{{ ref('CgAdd_Flag1') }}.CPL_TP_BROKER_GROUP_CAT_ID,
		{{ ref('CgAdd_Flag1') }}.CPL_FOUND_FLAG,
		{{ ref('CgAdd_Flag2') }}.APP_PROD_ID AS COM_APP_PROD_ID,
		{{ ref('CgAdd_Flag2') }}.COM_SUBTYPE_CODE,
		{{ ref('CgAdd_Flag2') }}.COM_APP_ID,
		{{ ref('CgAdd_Flag2') }}.COM_PRODUCT_TYPE_ID,
		{{ ref('CgAdd_Flag2') }}.COM_SM_CASE_ID,
		{{ ref('CgAdd_Flag2') }}.COM_FOUND_FLAG
	FROM {{ ref('CgAdd_Flag1') }}
	OUTER JOIN {{ ref('CgAdd_Flag2') }} ON {{ ref('CgAdd_Flag1') }}.APP_PROD_ID = {{ ref('CgAdd_Flag2') }}.APP_PROD_ID
)

SELECT * FROM JoinAppProd