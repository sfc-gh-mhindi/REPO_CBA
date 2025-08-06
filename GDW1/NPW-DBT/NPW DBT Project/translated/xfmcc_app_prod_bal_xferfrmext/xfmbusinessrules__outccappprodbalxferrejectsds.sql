{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH XfmBusinessRules__OutCCAppProdBalXferRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.BAL_XFER_OPTION_CAT_ID)) THEN (InXfmBusinessRules.BAL_XFER_OPTION_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.TRNF_OPTN_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BAL_XFER_OPTION_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.BAL_XFER_OPTION_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.TRNF_OPTN_C) AS svTrnfOptn,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.BAL_XFER_INSTITUTION_CAT_ID)) THEN (InXfmBusinessRules.BAL_XFER_INSTITUTION_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.CMPE_I,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BAL_XFER_INSTITUTION_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.BAL_XFER_INSTITUTION_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.CMPE_I) AS svCmpeI,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.XFER_AMT)) THEN (InXfmBusinessRules.XFER_AMT) ELSE ""))) = 0 AND Len(Trim(( IF IsNotNull((InXfmBusinessRules.BAL_XFER_OPTION_CAT_ID)) THEN (InXfmBusinessRules.BAL_XFER_OPTION_CAT_ID) ELSE ""))) = 0 AND Len(Trim(( IF IsNotNull((InXfmBusinessRules.BAL_XFER_INSTITUTION_CAT_ID)) THEN (InXfmBusinessRules.BAL_XFER_INSTITUTION_CAT_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.XFER_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.XFER_AMT, ''))) = 0
	    and LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BAL_XFER_OPTION_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.BAL_XFER_OPTION_CAT_ID, ''))) = 0
	    and LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BAL_XFER_INSTITUTION_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.BAL_XFER_INSTITUTION_CAT_ID, ''))) = 0, 
	    'N', 
	    'Y'
	) AS svLoadApptTrnfDetl,
		-- *SRC*: \(20)If svTrnfOptn = '9999' then 'RPR2102' else '',
		IFF(svTrnfOptn = '9999', 'RPR2102', '') AS svTrnfOptnErrorCode,
		-- *SRC*: \(20)If svCmpeI = '9999' then 'RPR2103' else '',
		IFF(svCmpeI = '9999', 'RPR2103', '') AS svCmpeErrorCode,
		-- *SRC*: \(20)If svTrnfOptnErrorCode <> "" AND svCmpeErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svTrnfOptnErrorCode <> '' AND svCmpeErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		-- *SRC*: \(20)If svTrnfOptnErrorCode <> "" Then svTrnfOptnErrorCode Else svCmpeErrorCode,
		IFF(svTrnfOptnErrorCode <> '', svTrnfOptnErrorCode, svCmpeErrorCode) AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag AND svLoadApptTrnfDetl = 'Y'
)

SELECT * FROM XfmBusinessRules__OutCCAppProdBalXferRejectsDS