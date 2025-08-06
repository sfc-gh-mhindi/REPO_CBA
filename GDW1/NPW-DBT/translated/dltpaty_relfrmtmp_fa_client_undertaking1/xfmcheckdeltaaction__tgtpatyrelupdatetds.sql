{{ config(materialized='view', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING1']) }}

WITH XfmCheckDeltaAction__TgtPatyRelUpdatetDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((JoinAllFAClientUndertaking.PATY_I)) THEN (JoinAllFAClientUndertaking.PATY_I) ELSE ""))) = 0) then 'RPR6206' else '',
		IFF(LEN(TRIM(IFF({{ ref('JoinAll') }}.PATY_I IS NOT NULL, {{ ref('JoinAll') }}.PATY_I, ''))) = 0, 'RPR6206', '') AS svErrorCode,
		{{ ref('JoinAll') }}.OLD_REL_I AS REL_I,
		{{ ref('JoinAll') }}.OLD_PATY_I AS PATY_I,
		{{ ref('JoinAll') }}.OLD_RELD_PATY_I AS RELD_PATY_I,
		{{ ref('JoinAll') }}.OLD_REL_LEVL_C AS REL_LEVL_C,
		{{ ref('JoinAll') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		-- *SRC*: AsInteger(REFR_PK),
		ASINTEGER(REFR_PK) AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE svErrorCode = '' AND {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__TgtPatyRelUpdatetDS