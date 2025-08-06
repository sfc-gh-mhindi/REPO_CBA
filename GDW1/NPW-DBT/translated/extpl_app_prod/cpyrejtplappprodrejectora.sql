{{ config(materialized='view', tags=['ExtPL_APP_PROD']) }}

WITH CpyRejtPLAppProdRejectOra AS (
	SELECT
		PL_APP_PROD_ID,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.PL_TARGET_CAT_ID AS PL_TARGET_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.REPAY_APPROX_AMT AS REPAY_APPROX_AMT_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.REPAY_FREQUENCY_ID AS REPAY_FREQUENCY_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.PL_APP_PROD_REPAY_CAT_ID AS PL_APP_PROD_REPAY_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.PL_PROD_TERM_CAT_ID AS PL_PROD_TERM_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.PL_CAMPAIGN_CAT_ID AS PL_CAMPAIGN_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.AD_HOC_CAMPAIGN_DESC AS AD_HOC_CAMPAIGN_DESC_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.CAR_SEEKER_FLAG AS CAR_SEEKER_FLAG_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.PL_PROD_TARGET_CAT_ID AS PL_PROD_TARGET_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.PL_MARKETING_SOURCE_CAT_ID AS PL_MARKETING_SOURCE_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.HLS_ACCT_NO AS HLS_ACCT_NO_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.TOTAL_INTEREST_AMT AS TOTAL_INTEREST_AMT_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.APP_PROD_AMT AS APP_PROD_AMT_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.TP_BROKER_ID AS TP_BROKER_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.TP_BROKER_FIRST_NAME AS TP_BROKER_FIRST_NAME_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.TP_BROKER_LAST_NAME AS TP_BROKER_LAST_NAME_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.TP_BROKER_GROUP_CAT_ID AS TP_BROKER_GROUP_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.READ_COSTS_AND_RISKS_FLAG AS READ_COSTS_AND_RISKS_FLAG_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.ACCEPTS_COSTS_AND_RISKS_DATE AS ACCEPTS_COSTS_AND_RISKS_DATE_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtPLAppProdRejectOra') }}
)

SELECT * FROM CpyRejtPLAppProdRejectOra