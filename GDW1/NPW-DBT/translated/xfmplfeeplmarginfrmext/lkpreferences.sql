{{ config(materialized='view', tags=['XfmPlFeePlMarginFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.PL_FEE_ID,
		{{ ref('CpyRename') }}.PL_APP_PROD_ID,
		{{ ref('CpyRename') }}.PL_FEE_PL_FEE_ID,
		{{ ref('CpyRename') }}.PL_FEE_ADD_TO_TOTAL_FLAG,
		{{ ref('CpyRename') }}.PL_FEE_FEE_AMT,
		{{ ref('CpyRename') }}.PL_FEE_BASE_FEE_AMT,
		{{ ref('CpyRename') }}.PL_FEE_PAY_FEE_AT_FUNDING_FLAG,
		{{ ref('CpyRename') }}.PL_FEE_START_DATE,
		{{ ref('CpyRename') }}.PL_CAPL_FEE_CAT_ID,
		{{ ref('CpyRename') }}.SRCE_CHAR_1_C,
		{{ ref('CpyRename') }}.PL_FEE_FEE_DESC,
		{{ ref('CpyRename') }}.PL_FEE_CASS_FEE_CODE,
		{{ ref('CpyRename') }}.PL_FEE_CASS_FEE_KEY,
		{{ ref('CpyRename') }}.PL_FEE_TOTAL_FEE_AMT,
		{{ ref('CpyRename') }}.PL_FEE_PL_APP_PROD_ID,
		{{ ref('CpyRename') }}.PL_MARGIN_PL_MARGIN_ID,
		{{ ref('CpyRename') }}.PL_MARGIN_MARGIN_AMT,
		{{ ref('CpyRename') }}.PL_MARGIN_PL_FEE_ID,
		{{ ref('CpyRename') }}.PL_MARGIN_PL_INT_RATE_ID,
		{{ ref('CpyRename') }}.MARG_REAS_CAT_ID,
		{{ ref('CpyRename') }}.PL_MARGIN_PL_APP_PROD_ID,
		{{ ref('CpyRename') }}.PL_FEE_FOUND_FLAG,
		{{ ref('CpyRename') }}.PL_MARGIN_FOUND_FLAG,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcGRD_GNRC_MAPLks') }}.TARG_CHAR_C,
		{{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_PLLks') }}.FEAT_OVRD_REAS_C,
		{{ ref('SrcMAP_CSE_FEE_CAPLLks') }}.FEE_CAPL_F
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAPLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_PLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_FEE_CAPLLks') }} ON 
)

SELECT * FROM LkpReferences