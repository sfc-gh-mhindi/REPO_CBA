{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

WITH SplitRejectTableRecs__InsertRejtTableRecs AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((OutJoinPlFeeId.REJT_FOUND_FLAG)) THEN (OutJoinPlFeeId.REJT_FOUND_FLAG) ELSE ('N')),
		IFF({{ ref('JN_PL_FEE_ID') }}.REJT_FOUND_FLAG IS NOT NULL, {{ ref('JN_PL_FEE_ID') }}.REJT_FOUND_FLAG, 'N') AS svRejectFoundFlag,
		-- *SRC*: \(20)If svRejectFoundFlag = 'N' And OutJoinPlFeeId.TGT_FOUND_FLAG = 'N' Then 'IGNORE' Else  If svRejectFoundFlag = 'Y' And ( IF IsNotNull((OutJoinPlFeeId.PL_FEE_FOUND_FLAG)) THEN (OutJoinPlFeeId.PL_FEE_FOUND_FLAG) ELSE "") = 'N' And OutJoinPlFeeId.TGT_FOUND_FLAG = 'N' Then 'DELETE' Else  If svRejectFoundFlag = 'Y' and OutJoinPlFeeId.TGT_FOUND_FLAG = 'Y' Then 'UPDATE' Else  If svRejectFoundFlag = 'N' And OutJoinPlFeeId.TGT_FOUND_FLAG = 'Y' Then 'INSERT' Else 'IGNORE',
		IFF(
	    svRejectFoundFlag = 'N' AND {{ ref('JN_PL_FEE_ID') }}.TGT_FOUND_FLAG = 'N', 'IGNORE',     
	    IFF(
	        svRejectFoundFlag = 'Y'
	    and IFF({{ ref('JN_PL_FEE_ID') }}.PL_FEE_FOUND_FLAG IS NOT NULL, {{ ref('JN_PL_FEE_ID') }}.PL_FEE_FOUND_FLAG, '') = 'N'
	    and {{ ref('JN_PL_FEE_ID') }}.TGT_FOUND_FLAG = 'N', 'DELETE', 
	        IFF(svRejectFoundFlag = 'Y'
	        and {{ ref('JN_PL_FEE_ID') }}.TGT_FOUND_FLAG = 'Y', 'UPDATE', IFF(svRejectFoundFlag = 'N'
	        and {{ ref('JN_PL_FEE_ID') }}.TGT_FOUND_FLAG = 'Y', 'INSERT', 'IGNORE'))
	    )
	) AS svStatus,
		PL_FEE_ID,
		{{ ref('JN_PL_FEE_ID') }}.APPT_PDCT_I AS PL_APP_PROD_ID,
		{{ ref('JN_PL_FEE_ID') }}.APPT_PDCT_I AS PL_FEE_PL_APP_PROD_ID,
		{{ ref('JN_PL_FEE_ID') }}.PL_MARGIN_ID AS PL_MARGIN_PL_MARGIN_ID,
		{{ ref('JN_PL_FEE_ID') }}.PL_FEE_ID AS PL_MARGIN_PL_FEE_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS PL_MARGIN_MARGIN_REASON_CAT_ID,
		'N' AS PL_FEE_FOUND_FLAG,
		'Y' AS PL_MARGIN_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		'RPR1111' AS EROR_C
	FROM {{ ref('JN_PL_FEE_ID') }}
	WHERE svStatus = 'INSERT'
)

SELECT * FROM SplitRejectTableRecs__InsertRejtTableRecs