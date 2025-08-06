{{ config(materialized='view', tags=['MergeChlBusFeeDisc']) }}

WITH Identify_HL_FEE_ID AS (
	SELECT
		-- *SRC*: \(20)If Not(IsNull(OutDropColumns.leftRec_HL_FEE_ID)) Then OutDropColumns.leftRec_HL_FEE_ID Else OutDropColumns.rightRec_HL_FEE_ID,
		IFF({{ ref('JoinBusFeeDisc') }}.leftRec_HL_FEE_ID IS NOT NULL, {{ ref('JoinBusFeeDisc') }}.leftRec_HL_FEE_ID, {{ ref('JoinBusFeeDisc') }}.rightRec_HL_FEE_ID) AS svHlFeeId,
		svHlFeeId AS HL_FEE_ID,
		{{ ref('JoinBusFeeDisc') }}.leftRec_HL_FEE_ID AS BF_HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		-- *SRC*: ( IF IsNotNull((OutDropColumns.BF_FOUND_FLAG)) THEN (OutDropColumns.BF_FOUND_FLAG) ELSE ("N")),
		IFF({{ ref('JoinBusFeeDisc') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinBusFeeDisc') }}.BF_FOUND_FLAG, 'N') AS BF_FOUND_FLAG,
		BFD_HL_FEE_DISCOUNT_ID,
		{{ ref('JoinBusFeeDisc') }}.rightRec_HL_FEE_ID AS BFD_HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		-- *SRC*: ( IF IsNotNull((OutDropColumns.BFD_FOUND_FLAG)) THEN (OutDropColumns.BFD_FOUND_FLAG) ELSE ("N")),
		IFF({{ ref('JoinBusFeeDisc') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinBusFeeDisc') }}.BFD_FOUND_FLAG, 'N') AS BFD_FOUND_FLAG
	FROM {{ ref('JoinBusFeeDisc') }}
	WHERE 
)

SELECT * FROM Identify_HL_FEE_ID