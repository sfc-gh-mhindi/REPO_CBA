{{ config(materialized='view', tags=['XfmChlIntRateChlRatePercChlProdIntMargFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CgRename') }}.HL_INT_RATE_ID,
		{{ ref('CgRename') }}.RATE_HL_INT_RATE_ID,
		{{ ref('CgRename') }}.RATE_HL_APP_PROD_ID,
		{{ ref('CgRename') }}.SRCE_CHAR_1_C,
		{{ ref('CgRename') }}.RATE_RATE_SEQUENCE,
		{{ ref('CgRename') }}.RATE_DURATION,
		{{ ref('CgRename') }}.PERC_HL_INT_RATE_ID,
		{{ ref('CgRename') }}.SRCE_CHAR_2_C,
		{{ ref('CgRename') }}.PERC_RATE_PERCENT_1,
		{{ ref('CgRename') }}.PERC_RATE_PERCENT_2,
		{{ ref('CgRename') }}.MARG_HL_PROD_INT_MARGIN_ID,
		{{ ref('CgRename') }}.MARG_HL_INT_RATE_ID,
		{{ ref('CgRename') }}.HL_PROD_INT_MARGIN_CAT_ID,
		{{ ref('CgRename') }}.MARG_MARGIN_TYPE,
		{{ ref('CgRename') }}.MARG_MARGIN_DESC,
		{{ ref('CgRename') }}.MARG_MARGIN_CODE,
		{{ ref('CgRename') }}.MARG_MARGIN_AMOUNT,
		{{ ref('CgRename') }}.MARG_ADJ_AMT,
		{{ ref('CgRename') }}.RATE_FOUND_FLAG,
		{{ ref('CgRename') }}.PERC_FOUND_FLAG,
		{{ ref('CgRename') }}.MARG_FOUND_FLAG,
		{{ ref('CgRename') }}.ORIG_ETL_D,
		{{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_SRCE_CHAR_2_CLks') }}.TARG_CHAR_C,
		{{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_HLLks') }}.FEAT_OVRD_REAS_C
	FROM {{ ref('CgRename') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_SRCE_CHAR_2_CLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_HLLks') }} ON 
)

SELECT * FROM LkpReferences