{{ config(materialized='view', tags=['ExtChlBusFeeBusFeeDisc']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.HL_FEE_ID)) THEN (XfmSeparateRejects.HL_FEE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.HL_FEE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.HL_FEE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		-- *SRC*: \(20)If DeltaFlag = 'S' Then XfmSeparateRejects.HL_FEE_ID Else XfmSeparateRejects.HL_FEE_ID_R,
		IFF(DeltaFlag = 'S', {{ ref('JoinSrcSortReject') }}.HL_FEE_ID, {{ ref('JoinSrcSortReject') }}.HL_FEE_ID_R) AS HL_FEE_ID,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_HL_FEE_ID Else XfmSeparateRejects.BF_HL_FEE_ID_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_HL_FEE_ID, {{ ref('JoinSrcSortReject') }}.BF_HL_FEE_ID_R) AS BF_HL_FEE_ID,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_HL_APP_PROD_ID Else XfmSeparateRejects.BF_HL_APP_PROD_ID_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_HL_APP_PROD_ID, {{ ref('JoinSrcSortReject') }}.BF_HL_APP_PROD_ID_R) AS BF_HL_APP_PROD_ID,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_XML_CODE Else XfmSeparateRejects.BF_XML_CODE_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_XML_CODE, {{ ref('JoinSrcSortReject') }}.BF_XML_CODE_R) AS BF_XML_CODE,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_DISPLAY_NAME Else XfmSeparateRejects.BF_DISPLAY_NAME_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_DISPLAY_NAME, {{ ref('JoinSrcSortReject') }}.BF_DISPLAY_NAME_R) AS BF_DISPLAY_NAME,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_CATEGORY Else XfmSeparateRejects.BF_CATEGORY_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_CATEGORY, {{ ref('JoinSrcSortReject') }}.BF_CATEGORY_R) AS BF_CATEGORY,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_UNIT_AMOUNT Else XfmSeparateRejects.BF_UNIT_AMOUNT_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_UNIT_AMOUNT, {{ ref('JoinSrcSortReject') }}.BF_UNIT_AMOUNT_R) AS BF_UNIT_AMOUNT,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_TOTAL_AMOUNT Else XfmSeparateRejects.BF_TOTAL_AMOUNT_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_TOTAL_AMOUNT, {{ ref('JoinSrcSortReject') }}.BF_TOTAL_AMOUNT_R) AS BF_TOTAL_AMOUNT,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_HL_FEE_DISCOUNT_ID Else XfmSeparateRejects.BFD_HL_FEE_DISCOUNT_ID_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_HL_FEE_DISCOUNT_ID, {{ ref('JoinSrcSortReject') }}.BFD_HL_FEE_DISCOUNT_ID_R) AS BFD_HL_FEE_DISCOUNT_ID,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_HL_FEE_ID Else XfmSeparateRejects.BFD_HL_FEE_ID_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_HL_FEE_ID, {{ ref('JoinSrcSortReject') }}.BFD_HL_FEE_ID_R) AS BFD_HL_FEE_ID,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_DISCOUNT_REASON Else XfmSeparateRejects.BFD_DISCOUNT_REASON_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_REASON, {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_REASON_R) AS BFD_DISCOUNT_REASON,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_DISCOUNT_CODE Else XfmSeparateRejects.BFD_DISCOUNT_CODE_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_CODE, {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_CODE_R) AS BFD_DISCOUNT_CODE,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_DISCOUNT_AMT Else XfmSeparateRejects.BFD_DISCOUNT_AMT_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_AMT, {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_AMT_R) AS BFD_DISCOUNT_AMT,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_DISCOUNT_TERM Else XfmSeparateRejects.BFD_DISCOUNT_TERM_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_TERM, {{ ref('JoinSrcSortReject') }}.BFD_DISCOUNT_TERM_R) AS BFD_DISCOUNT_TERM,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_HL_FEE_DISCOUNT_CAT_ID Else XfmSeparateRejects.BFD_HL_FEE_DISCOUNT_CAT_ID_R,
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_HL_FEE_DISCOUNT_CAT_ID, {{ ref('JoinSrcSortReject') }}.BFD_HL_FEE_DISCOUNT_CAT_ID_R) AS BFD_HL_FEE_DISCOUNT_CAT_ID,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG)) THEN (XfmSeparateRejects.BF_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BF_FOUND_FLAG Else  If ( IF IsNotNull((XfmSeparateRejects.BF_FOUND_FLAG_R)) THEN (XfmSeparateRejects.BF_FOUND_FLAG_R) ELSE "") = 'Y' Then XfmSeparateRejects.BF_FOUND_FLAG_R Else 'N',
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG, IFF(IFF({{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG_R IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG_R, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BF_FOUND_FLAG_R, 'N')) AS BF_FOUND_FLAG,
		-- *SRC*: \(20)If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_FOUND_FLAG Else  If ( IF IsNotNull((XfmSeparateRejects.BFD_FOUND_FLAG_R)) THEN (XfmSeparateRejects.BFD_FOUND_FLAG_R) ELSE "") = 'Y' Then XfmSeparateRejects.BFD_FOUND_FLAG_R Else 'N',
		IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG, IFF(IFF({{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG_R IS NOT NULL, {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG_R, '') = 'Y', {{ ref('JoinSrcSortReject') }}.BFD_FOUND_FLAG_R, 'N')) AS BFD_FOUND_FLAG,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmSeparateRejects.ORIG_ETL_D_R)) THEN (XfmSeparateRejects.ORIG_ETL_D_R) ELSE ""))) = 0 Then ETL_PROCESS_DT Else XfmSeparateRejects.ORIG_ETL_D_R,
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R IS NOT NULL, {{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R, ''))) = 0, ETL_PROCESS_DT, {{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R) AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE 
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest