{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH CpyPLAppProdSeq AS (
	SELECT
		APP_ID,
		{{ ref('MergeDS') }}.HLM_APP__ACCOUNT_ID AS HLM_ACCOUNT_ID,
		{{ ref('MergeDS') }}.HLM_APP_ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
		{{ ref('MergeDS') }}.HLM_APP_CRIS_PRODUCT_ID AS CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		{{ ref('MergeDS') }}.HLM_APP_DISCHARGE_REASON_ID AS DISCHARGE_REASON_ID,
		{{ ref('MergeDS') }}.HLM_APP_PROD_ID AS HL_APP_PROD_ID
	FROM {{ ref('MergeDS') }}
)

SELECT * FROM CpyPLAppProdSeq