{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptTrnfDetlDS AS (
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
		-- *SRC*: 'CSE' : 'CC' : InXfmBusinessRules.CC_APP_ID,
		CONCAT(CONCAT('CSE', 'CC'), {{ ref('ModNullHandling') }}.CC_APP_ID) AS APPT_I,
		-- *SRC*: 'CSECC' : InXfmBusinessRules.CC_APP_PROD_BAL_XFER_ID,
		CONCAT('CSECC', {{ ref('ModNullHandling') }}.CC_APP_PROD_BAL_XFER_ID) AS APPT_TRNF_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: \(20)If (svTrnfOptn = DEFAULT_NULL_VALUE) then SetNull() else svTrnfOptn,
		IFF(svTrnfOptn = DEFAULT_NULL_VALUE, SETNULL(), svTrnfOptn) AS TRNF_OPTN_C,
		{{ ref('ModNullHandling') }}.XFER_AMT AS TRNF_A,
		'AUD' AS CNCY_C,
		-- *SRC*: \(20)If (svCmpeI = DEFAULT_NULL_VALUE) then SetNull() else svCmpeI,
		IFF(svCmpeI = DEFAULT_NULL_VALUE, SETNULL(), svCmpeI) AS CMPE_I,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadApptTrnfDetl = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptTrnfDetlDS