{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmHlProdIntMargin']) }}

WITH SplitRejectTableRecs__UpdateRejtTableRecs AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((OutJoinHlIntRateId.REJT_FOUND_FLAG)) THEN (OutJoinHlIntRateId.REJT_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JN_HL_INT_RATE_ID') }}.REJT_FOUND_FLAG IS NOT NULL, {{ ref('JN_HL_INT_RATE_ID') }}.REJT_FOUND_FLAG, 'N') AS svRejectFoundFlag,
		-- *SRC*: \(20)If svRejectFoundFlag = 'N' And OutJoinHlIntRateId.TGT_FOUND_FLAG = 'N' Then 'IGNORE' Else  If svRejectFoundFlag = 'Y' And ( IF IsNotNull((OutJoinHlIntRateId.RATE_FOUND_FLAG)) THEN (OutJoinHlIntRateId.RATE_FOUND_FLAG) ELSE "") = 'N' And ( IF IsNotNull((OutJoinHlIntRateId.PERC_FOUND_FLAG)) THEN (OutJoinHlIntRateId.PERC_FOUND_FLAG) ELSE "") = 'N' And OutJoinHlIntRateId.TGT_FOUND_FLAG = 'N' Then 'DELETE' Else  If svRejectFoundFlag = 'Y' and OutJoinHlIntRateId.TGT_FOUND_FLAG = 'Y' Then 'UPDATE' Else  If svRejectFoundFlag = 'N' And OutJoinHlIntRateId.TGT_FOUND_FLAG = 'Y' Then 'INSERT' Else 'IGNORE',
		IFF(
	    svRejectFoundFlag = 'N' AND {{ ref('JN_HL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'N', 'IGNORE',     
	    IFF(
	        svRejectFoundFlag = 'Y'
	    and IFF({{ ref('JN_HL_INT_RATE_ID') }}.RATE_FOUND_FLAG IS NOT NULL, {{ ref('JN_HL_INT_RATE_ID') }}.RATE_FOUND_FLAG, '') = 'N'
	    and IFF({{ ref('JN_HL_INT_RATE_ID') }}.PERC_FOUND_FLAG IS NOT NULL, {{ ref('JN_HL_INT_RATE_ID') }}.PERC_FOUND_FLAG, '') = 'N'
	    and {{ ref('JN_HL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'N', 'DELETE', 
	        IFF(svRejectFoundFlag = 'Y'
	        and {{ ref('JN_HL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'Y', 'UPDATE', IFF(svRejectFoundFlag = 'N'
	        and {{ ref('JN_HL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'Y', 'INSERT', 'IGNORE'))
	    )
	) AS svStatus,
		HL_INT_RATE_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS MARG_HL_PROD_INT_MARGIN_CAT_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS MARG_MARGIN_TYPE,
		-- *SRC*: SetNull(),
		SETNULL() AS MARG_MARGIN_DESC,
		-- *SRC*: SetNull(),
		SETNULL() AS MARG_MARGIN_CODE,
		-- *SRC*: SetNull(),
		SETNULL() AS MARG_MARGIN_AMOUNT,
		-- *SRC*: SetNull(),
		SETNULL() AS MARG_ADJ_AMT,
		'Y' AS MARG_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D
	FROM {{ ref('JN_HL_INT_RATE_ID') }}
	WHERE svStatus = 'UPDATE'
)

SELECT * FROM SplitRejectTableRecs__UpdateRejtTableRecs