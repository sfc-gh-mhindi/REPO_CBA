{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH XfmBusinessRules__OutApptPdctAcct AS (
	SELECT
		-- *SRC*: \(20)if (len(trim(( IF IsNotNull((InModNullHandling.ACCT_QLFY_C)) THEN (InModNullHandling.ACCT_QLFY_C) ELSE ""))) <> 0 or len(trim(( IF IsNotNull((InModNullHandling.SRCE_SYST_C)) THEN (InModNullHandling.SRCE_SYST_C) ELSE ""))) <> 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.ACCT_QLFY_C IS NOT NULL, {{ ref('LkpReferences') }}.ACCT_QLFY_C, ''))) <> 0 OR LEN(TRIM(IFF({{ ref('LkpReferences') }}.SRCE_SYST_C IS NOT NULL, {{ ref('LkpReferences') }}.SRCE_SYST_C, ''))) <> 0, 'Y', 'N') AS LoadAdrs,
		-- *SRC*: "CSEHM" : InModNullHandling.HL_APP_PROD_ID,
		CONCAT('CSEHM', {{ ref('LkpReferences') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: InModNullHandling.SRCE_SYST_C : InModNullHandling.ACCT_QLFY_C : InModNullHandling.ACCOUNT_NUMBER,
		CONCAT(CONCAT({{ ref('LkpReferences') }}.SRCE_SYST_C, {{ ref('LkpReferences') }}.ACCT_QLFY_C), {{ ref('LkpReferences') }}.ACCOUNT_NUMBER) AS ACCT_I,
		'HLMC' AS REL_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('LkpReferences') }}
	WHERE LoadAdrs = 'Y'
)

SELECT * FROM XfmBusinessRules__OutApptPdctAcct