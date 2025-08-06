{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmHlFeeDiscount']) }}

WITH JN_HL_FEE_ID AS (
	SELECT
		{{ ref('DetermineTargetFoundFlag') }}.TGT_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.SRC_TBL_NAME,
		{{ ref('DetermineTargetFoundFlag') }}.HL_FEE_ID,
		{{ ref('DetermineTargetFoundFlag') }}.SRCE_SYST_STND_VALU_A,
		{{ ref('DetermineTargetFoundFlag') }}.TGT_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.BF_FOUND_FLAG,
		{{ ref('SrcRejectOra') }}.REJT_FOUND_FLAG
	FROM {{ ref('DetermineTargetFoundFlag') }}
	LEFT JOIN {{ ref('SrcRejectOra') }} ON {{ ref('DetermineTargetFoundFlag') }}.HL_FEE_ID = {{ ref('SrcRejectOra') }}.HL_FEE_ID
)

SELECT * FROM JN_HL_FEE_ID