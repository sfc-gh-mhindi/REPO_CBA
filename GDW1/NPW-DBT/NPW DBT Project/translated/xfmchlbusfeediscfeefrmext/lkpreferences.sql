{{ config(materialized='view', tags=['XfmChlBusFeeDiscFeeFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('AddColMapTypeLkp') }}.HL_FEE_ID,
		{{ ref('AddColMapTypeLkp') }}.BF_HL_FEE_ID,
		{{ ref('AddColMapTypeLkp') }}.BF_HL_APP_PROD_ID,
		{{ ref('AddColMapTypeLkp') }}.SRCE_CHAR_1_C,
		{{ ref('AddColMapTypeLkp') }}.BF_DISPLAY_NAME,
		{{ ref('AddColMapTypeLkp') }}.BF_CATEGORY,
		{{ ref('AddColMapTypeLkp') }}.BF_UNIT_AMOUNT,
		{{ ref('AddColMapTypeLkp') }}.BF_TOTAL_AMOUNT,
		{{ ref('AddColMapTypeLkp') }}.BF_FOUND_FLAG,
		{{ ref('AddColMapTypeLkp') }}.BFD_HL_FEE_DISCOUNT_ID,
		{{ ref('AddColMapTypeLkp') }}.BFD_HL_FEE_ID,
		{{ ref('AddColMapTypeLkp') }}.BFD_DISCOUNT_REASON,
		{{ ref('AddColMapTypeLkp') }}.BFD_DISCOUNT_CODE,
		{{ ref('AddColMapTypeLkp') }}.BFD_DISCOUNT_AMT,
		{{ ref('AddColMapTypeLkp') }}.BFD_DISCOUNT_TERM,
		{{ ref('AddColMapTypeLkp') }}.HL_FEE_DISCOUNT_CAT_ID AS BFD_HL_FEE_DISCOUNT_CAT_ID,
		{{ ref('AddColMapTypeLkp') }}.BFD_FOUND_FLAG,
		{{ ref('AddColMapTypeLkp') }}.ORIG_ETL_D,
		{{ ref('AddColMapTypeLkp') }}.MAP_TYPE_C,
		{{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_CLks') }}.TARG_CHAR_C,
		{{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_HL_D') }}.OVRD_REAS_C
	FROM {{ ref('AddColMapTypeLkp') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_SRCE_CHAR_1_CLks') }} ON 
	LEFT JOIN {{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_HL_D') }} ON 
)

SELECT * FROM LkpReferences