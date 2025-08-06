{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

WITH SplitRejectTableRecs__InsertRejtTableRecs AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((OutJoinPlIntRateId.REJT_FOUND_FLAG)) THEN (OutJoinPlIntRateId.REJT_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JN_PL_INT_RATE_ID') }}.REJT_FOUND_FLAG IS NOT NULL, {{ ref('JN_PL_INT_RATE_ID') }}.REJT_FOUND_FLAG, 'N') AS svRejectFoundFlag,
		-- *SRC*: \(20)If svRejectFoundFlag = 'N' And OutJoinPlIntRateId.TGT_FOUND_FLAG = 'N' Then 'IGNORE' Else  If svRejectFoundFlag = 'Y' And ( IF IsNotNull((OutJoinPlIntRateId.PL_INT_RATE_FOUND_FLAG)) THEN (OutJoinPlIntRateId.PL_INT_RATE_FOUND_FLAG) ELSE "") = 'N' And ( IF IsNotNull((OutJoinPlIntRateId.PL_INT_RATE_AMT_FOUND_FLAG)) THEN (OutJoinPlIntRateId.PL_INT_RATE_AMT_FOUND_FLAG) ELSE "") = 'N' And OutJoinPlIntRateId.TGT_FOUND_FLAG = 'N' Then 'DELETE' Else  If svRejectFoundFlag = 'Y' and OutJoinPlIntRateId.TGT_FOUND_FLAG = 'Y' Then 'UPDATE' Else  If svRejectFoundFlag = 'N' And OutJoinPlIntRateId.TGT_FOUND_FLAG = 'Y' Then 'INSERT' Else 'IGNORE',
		IFF(
	    svRejectFoundFlag = 'N' AND {{ ref('JN_PL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'N', 'IGNORE',     
	    IFF(        
	        svRejectFoundFlag = 'Y'
	        and IFF({{ ref('JN_PL_INT_RATE_ID') }}.PL_INT_RATE_FOUND_FLAG IS NOT NULL, {{ ref('JN_PL_INT_RATE_ID') }}.PL_INT_RATE_FOUND_FLAG, '') = 'N'
	        and IFF({{ ref('JN_PL_INT_RATE_ID') }}.PL_INT_RATE_AMT_FOUND_FLAG IS NOT NULL, {{ ref('JN_PL_INT_RATE_ID') }}.PL_INT_RATE_AMT_FOUND_FLAG, '') = 'N'
	        and {{ ref('JN_PL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'N', 
	        'DELETE', 
	        IFF(svRejectFoundFlag = 'Y'
	        and {{ ref('JN_PL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'Y', 'UPDATE', IFF(svRejectFoundFlag = 'N'
	        and {{ ref('JN_PL_INT_RATE_ID') }}.TGT_FOUND_FLAG = 'Y', 'INSERT', 'IGNORE'))
	    )
	) AS svStatus,
		PL_INT_RATE_ID,
		{{ ref('JN_PL_INT_RATE_ID') }}.PL_MARGIN_ID AS PL_MARGIN_PL_MARGIN_ID,
		{{ ref('JN_PL_INT_RATE_ID') }}.PL_INT_RATE_ID AS PL_MARGIN_PL_INT_RATE_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS PL_MARGIN_MARGIN_RESN_CAT_ID,
		'Y' AS PL_MARGIN_FOUND_FLAG,
		'N' AS PL_INT_RATE_FOUND_FLAG,
		'N' AS PL_INT_RATE_AMT_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		'RPR1110' AS EROR_C
	FROM {{ ref('JN_PL_INT_RATE_ID') }}
	WHERE svStatus = 'INSERT'
)

SELECT * FROM SplitRejectTableRecs__InsertRejtTableRecs