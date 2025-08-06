{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('Transformer_212') }}.CC_APP_PROD_ID,
		{{ ref('Transformer_212') }}.REQUESTED_LIMIT_AMT,
		{{ ref('Transformer_212') }}.SRCE_NUMC_1_C,
		{{ ref('Transformer_212') }}.CBA_HOMELOAN_NO,
		{{ ref('Transformer_212') }}.PRE_APPRV_AMOUNT,
		{{ ref('Transformer_212') }}.ORIG_ETL_D,
		{{ ref('SrcGRD_GNRC_MAP_SRCE_NUM_1_CLks') }}.TARG_CHAR_C,
		{{ ref('Transformer_212') }}.READ_COSTS_AND_RISKS_FLAG,
		{{ ref('Transformer_212') }}.ACCEPTS_COSTS_AND_RISKS_DATE
	FROM {{ ref('Transformer_212') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_SRCE_NUM_1_CLks') }} ON 
)

SELECT * FROM LkpReferences