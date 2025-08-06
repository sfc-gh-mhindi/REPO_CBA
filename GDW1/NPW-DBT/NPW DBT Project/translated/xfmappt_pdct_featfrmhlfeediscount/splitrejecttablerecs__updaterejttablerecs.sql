{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmHlFeeDiscount']) }}

WITH SplitRejectTableRecs__UpdateRejtTableRecs AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((OutJoinHlFeeId.REJT_FOUND_FLAG)) THEN (OutJoinHlFeeId.REJT_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JN_HL_FEE_ID') }}.REJT_FOUND_FLAG IS NOT NULL, {{ ref('JN_HL_FEE_ID') }}.REJT_FOUND_FLAG, 'N') AS svRejectFoundFlag,
		-- *SRC*: \(20)If svRejectFoundFlag = 'N' And OutJoinHlFeeId.TGT_FOUND_FLAG = 'N' Then 'IGNORE' Else  If svRejectFoundFlag = 'Y' And ( IF IsNotNull((OutJoinHlFeeId.BF_FOUND_FLAG)) THEN (OutJoinHlFeeId.BF_FOUND_FLAG) ELSE "") = 'N' And OutJoinHlFeeId.TGT_FOUND_FLAG = 'N' Then 'DELETE' Else  If svRejectFoundFlag = 'Y' and OutJoinHlFeeId.TGT_FOUND_FLAG = 'Y' Then 'UPDATE' Else  If svRejectFoundFlag = 'N' And OutJoinHlFeeId.TGT_FOUND_FLAG = 'Y' Then 'INSERT' Else 'IGNORE',
		IFF(
	    svRejectFoundFlag = 'N' AND {{ ref('JN_HL_FEE_ID') }}.TGT_FOUND_FLAG = 'N', 'IGNORE',     
	    IFF(
	        svRejectFoundFlag = 'Y'
	    and IFF({{ ref('JN_HL_FEE_ID') }}.BF_FOUND_FLAG IS NOT NULL, {{ ref('JN_HL_FEE_ID') }}.BF_FOUND_FLAG, '') = 'N'
	    and {{ ref('JN_HL_FEE_ID') }}.TGT_FOUND_FLAG = 'N', 'DELETE', 
	        IFF(svRejectFoundFlag = 'Y'
	        and {{ ref('JN_HL_FEE_ID') }}.TGT_FOUND_FLAG = 'Y', 'UPDATE', IFF(svRejectFoundFlag = 'N'
	        and {{ ref('JN_HL_FEE_ID') }}.TGT_FOUND_FLAG = 'Y', 'INSERT', 'IGNORE'))
	    )
	) AS svStatus,
		-- *SRC*: \(20)If Len(( IF IsNotNull((OutJoinHlFeeId.SRCE_SYST_STND_VALU_A)) THEN (OutJoinHlFeeId.SRCE_SYST_STND_VALU_A) ELSE "")) = 0 Then DEFAULT_NULL_VALUE Else Trim(Trim(Trim(Trim(OutJoinHlFeeId.SRCE_SYST_STND_VALU_A), 0, 'L'), 0, 'T'), '.', 'T'),
		IFF(LEN(IFF({{ ref('JN_HL_FEE_ID') }}.SRCE_SYST_STND_VALU_A IS NOT NULL, {{ ref('JN_HL_FEE_ID') }}.SRCE_SYST_STND_VALU_A, '')) = 0, DEFAULT_NULL_VALUE, TRIM(TRIM(TRIM(TRIM({{ ref('JN_HL_FEE_ID') }}.SRCE_SYST_STND_VALU_A), 0, 'L'), 0, 'T'), '.', 'T')) AS svConvertAmtToString,
		HL_FEE_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS BFD_DISCOUNT_REASON,
		-- *SRC*: SetNull(),
		SETNULL() AS BFD_DISCOUNT_CODE,
		-- *SRC*: \(20)If svConvertAmtToString = DEFAULT_NULL_VALUE Then SetNull() Else  If Len(svConvertAmtToString) = 0 Then '0' Else svConvertAmtToString,
		IFF(svConvertAmtToString = DEFAULT_NULL_VALUE, SETNULL(), IFF(LEN(svConvertAmtToString) = 0, '0', svConvertAmtToString)) AS BFD_DISCOUNT_AMT,
		-- *SRC*: SetNull(),
		SETNULL() AS BFD_DISCOUNT_TERM,
		-- *SRC*: SetNull(),
		SETNULL() AS BFD_HL_FEE_DISCOUNT_CAT_ID,
		'Y' AS BFD_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D
	FROM {{ ref('JN_HL_FEE_ID') }}
	WHERE svStatus = 'UPDATE'
)

SELECT * FROM SplitRejectTableRecs__UpdateRejtTableRecs