{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH XfmBusinessRules__OutTmpAppProdPurpDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_PURP_CAT_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_PURP_CAT_ID) ELSE ""))) = 0) Then 'UNKN' ELSE InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_CAT_ID, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS svApptPurp,
		-- *SRC*: \(20)If svApptPurp = '9999' then 'RPR2105' else  if svApptPurp = DEFAULT_NULL_VALUE then 'REJ2008' else '',
		IFF(svApptPurp = '9999', 'RPR2105', IFF(svApptPurp = DEFAULT_NULL_VALUE, 'REJ2008', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: 'CSEPL' : InXfmBusinessRules.PL_APP_PROD_ID,
		CONCAT('CSEPL', {{ ref('ModNullHandling') }}.PL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_ID AS SRCE_SYST_APPT_PDCT_PURP_I,
		svApptPurp AS PURP_TYPE_C,
		-- *SRC*: SetNull(),
		SETNULL() AS PURP_CLAS_C,
		'CSE' AS SRCE_SYST_C,
		{{ ref('ModNullHandling') }}.AMT AS PURP_A,
		'AUD' AS CNCY_C,
		'' AS MAIN_PURP_F,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE 
)

SELECT * FROM XfmBusinessRules__OutTmpAppProdPurpDS