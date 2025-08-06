{{ config(materialized='view', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('Transformer_225') }}.PL_INT_RATE_ID,
		{{ ref('Transformer_225') }}.PL_INT_RATE_PL_INT_RATE_ID,
		{{ ref('Transformer_225') }}.PL_INT_RATE_DOC_SEQ_NO,
		{{ ref('Transformer_225') }}.PL_INT_RATE_CASS_MARGIN_AMT,
		{{ ref('Transformer_225') }}.PL_INT_RATE_INT_RATE_TERM,
		{{ ref('Transformer_225') }}.SRCE_NUMC_2_C AS PL_INT_RATE_INT_RATE_FREQ_ID,
		{{ ref('Transformer_225') }}.SRCE_NUMC_1_C AS PL_INT_RATE_VARIANT_CAT_ID,
		{{ ref('Transformer_225') }}.PL_INT_RATE_USAGE_CAT_ID,
		{{ ref('Transformer_225') }}.PL_INT_RATE_PL_APP_PROD_ID,
		{{ ref('Transformer_225') }}.PL_MARGIN_PL_MARGIN_ID,
		{{ ref('Transformer_225') }}.PL_MARGIN_MARGIN_AMT,
		{{ ref('Transformer_225') }}.PL_MARGIN_PL_FEE_ID,
		{{ ref('Transformer_225') }}.PL_MARGIN_PL_INT_RATE_ID,
		{{ ref('Transformer_225') }}.MARG_REAS_CAT_ID,
		{{ ref('Transformer_225') }}.PL_MARGIN_PL_APP_PROD_ID,
		{{ ref('Transformer_225') }}.PL_INT_RATE_AMT_INT_RTE_AMT_2,
		{{ ref('Transformer_225') }}.PL_INT_RATE_AMT_INT_RTCT_ID2,
		{{ ref('Transformer_225') }}.PL_INT_RATE_AMT_PL_INT_RATE_ID,
		{{ ref('Transformer_225') }}.PL_INT_RATE_AMT_INT_RATE_AMT_1,
		{{ ref('Transformer_225') }}.PL_INT_RATE_AMT_INT_RTCT_ID1,
		{{ ref('Transformer_225') }}.PL_MARGIN_FOUND_FLAG,
		{{ ref('Transformer_225') }}.PL_INT_RATE_FOUND_FLAG,
		{{ ref('Transformer_225') }}.PL_INT_RATE_AMT_FOUND_FLAG,
		{{ ref('Transformer_225') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_TYPE_FEAT_OVRD_REAS_PLLks') }}.FEAT_OVRD_REAS_C,
		{{ ref('SrcMAP_GRD_GNRC_MAP_FEAT_ILks') }}.TARG_CHAR_C
	FROM {{ ref('Transformer_225') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_TYPE_FEAT_OVRD_REAS_PLLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_GRD_GNRC_MAP_FEAT_ILks') }} ON 
)

SELECT * FROM LkpReferences