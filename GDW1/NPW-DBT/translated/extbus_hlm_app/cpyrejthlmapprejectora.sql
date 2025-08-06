{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH CpyRejtHlmAppRejectOra AS (
	SELECT
		{{ ref('SrcRejtPLAppProdRejectOra') }}.HLM_APP_ID AS APP_ID,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.HLM_ACCOUNT_ID AS HLM_ACCOUNT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.ACCOUNT_NUMBER AS ACCOUNT_NUMBER_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.CRIS_PRODUCT_ID AS CRIS_PRODUCT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.HLM_APP_TYPE_CAT_ID AS HLM_APP_TYPE_CAT_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.DISCHARGE_REASON_ID AS DISCHARGE_REASON_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.HL_APP_PROD_ID AS HL_APP_PROD_ID_R,
		{{ ref('SrcRejtPLAppProdRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtPLAppProdRejectOra') }}
)

SELECT * FROM CpyRejtHlmAppRejectOra