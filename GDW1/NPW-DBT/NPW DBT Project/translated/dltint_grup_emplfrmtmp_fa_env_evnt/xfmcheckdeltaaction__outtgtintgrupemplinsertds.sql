{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtIntGrupEmplInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: \(20)If DSLink84.change_code = INSERT then 'RPR6202' else '',
		IFF({{ ref('Join_82') }}.change_code = INSERT, 'RPR6202', '') AS svErrorCode,
		-- *SRC*: ( IF IsNotNull((DSLink84.INT_GRUP_I)) THEN (DSLink84.INT_GRUP_I) ELSE ""),
		IFF({{ ref('Join_82') }}.INT_GRUP_I IS NOT NULL, {{ ref('Join_82') }}.INT_GRUP_I, '') AS INT_GRUP_I,
		-- *SRC*: ( IF IsNotNull((DSLink84.EMPL_I)) THEN (DSLink84.EMPL_I) ELSE ""),
		IFF({{ ref('Join_82') }}.EMPL_I IS NOT NULL, {{ ref('Join_82') }}.EMPL_I, '') AS EMPL_I,
		-- *SRC*: ( IF IsNotNull((DSLink84.EMPL_ROLE_C)) THEN (DSLink84.EMPL_ROLE_C) ELSE ""),
		IFF({{ ref('Join_82') }}.EMPL_ROLE_C IS NOT NULL, {{ ref('Join_82') }}.EMPL_ROLE_C, '') AS EMPL_ROLE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join_82') }}
	WHERE {{ ref('Join_82') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtIntGrupEmplInsertDS