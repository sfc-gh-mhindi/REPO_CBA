{{ config(materialized='view', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING2']) }}

WITH XfmCheckDeltaAction__TgtPatyRelInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((JoinAllFAClientUndertaking.PATY_I)) THEN (JoinAllFAClientUndertaking.PATY_I) ELSE ""))) = 0) then 'RPR6207' else '',
		IFF(LEN(TRIM(IFF({{ ref('JoinAll') }}.PATY_I IS NOT NULL, {{ ref('JoinAll') }}.PATY_I, ''))) = 0, 'RPR6207', '') AS svErrorCode,
		PATY_I,
		RELD_PATY_I,
		REL_I,
		REL_TYPE_C,
		SRCE_SYST_C,
		PATY_ROLE_C,
		REL_STUS_C,
		REL_REAS_C,
		REL_LEVL_C,
		REL_EFFT_D,
		REL_EXPY_D,
		SRCE_SYST_REL_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		-- *SRC*: AsInteger(REFR_PK),
		ASINTEGER(REFR_PK) AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('JoinAll') }}
	WHERE svErrorCode = '' AND {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__TgtPatyRelInsertDS