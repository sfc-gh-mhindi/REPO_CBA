{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.APP_ID,
		{{ ref('CpyRename') }}.HLM_ACCOUNT_ID,
		{{ ref('CpyRename') }}.ACCOUNT_NUMBER,
		{{ ref('CpyRename') }}.CRIS_PRODUCT_ID,
		{{ ref('CpyRename') }}.HLM_APP_TYPE_CAT_ID,
		{{ ref('CpyRename') }}.DCHG_REAS_ID,
		{{ ref('CpyRename') }}.HL_APP_PROD_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_CRIS_PDCT_Tera') }}.ACCT_QLFY_C,
		{{ ref('SrcMAP_CSE_CRIS_PDCT_Tera') }}.SRCE_SYST_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_CRIS_PDCT_Tera') }} ON {{ ref('CpyRename') }}.CRIS_PRODUCT_ID = {{ ref('SrcMAP_CSE_CRIS_PDCT_Tera') }}.CRIS_PDCT_ID
)

SELECT * FROM LkpReferences