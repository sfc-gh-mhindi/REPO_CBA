{{ config(materialized='view', tags=['ExtChlBusFeeBusFeeDisc']) }}

WITH XfmCheckIdNull__OutXfmCheckIdNull AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutCopyNoOp.HL_FEE_ID)) THEN (OutCopyNoOp.HL_FEE_ID) ELSE ""))) = 0) Then 'REJ4102' Else '',
		IFF(LEN(TRIM(IFF({{ ref('Cpy_NoOp') }}.HL_FEE_ID IS NOT NULL, {{ ref('Cpy_NoOp') }}.HL_FEE_ID, ''))) = 0, 'REJ4102', '') AS ErrorCode,
		HL_FEE_ID,
		BF_HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		BFD_HL_FEE_DISCOUNT_ID,
		BFD_HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BF_FOUND_FLAG,
		BFD_FOUND_FLAG
	FROM {{ ref('Cpy_NoOp') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckIdNull__OutXfmCheckIdNull