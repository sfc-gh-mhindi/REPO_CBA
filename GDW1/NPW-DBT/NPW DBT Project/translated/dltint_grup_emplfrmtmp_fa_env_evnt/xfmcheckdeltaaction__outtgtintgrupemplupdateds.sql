{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtIntGrupEmplUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: \(20)If DSLink84.change_code = INSERT then 'RPR6202' else '',
		IFF({{ ref('Join_82') }}.change_code = INSERT, 'RPR6202', '') AS svErrorCode,
		-- *SRC*: ( IF IsNotNull((DSLink84.OLD_INT_GRUP_I)) THEN (DSLink84.OLD_INT_GRUP_I) ELSE ""),
		IFF({{ ref('Join_82') }}.OLD_INT_GRUP_I IS NOT NULL, {{ ref('Join_82') }}.OLD_INT_GRUP_I, '') AS INT_GRUP_I,
		-- *SRC*: ( IF IsNotNull((DSLink84.OLD_EMPL_I)) THEN (DSLink84.OLD_EMPL_I) ELSE ""),
		IFF({{ ref('Join_82') }}.OLD_EMPL_I IS NOT NULL, {{ ref('Join_82') }}.OLD_EMPL_I, '') AS EMPL_I,
		-- *SRC*: ( IF IsNotNull((DSLink84.OLD_EFFT_D)) THEN (DSLink84.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('Join_82') }}.OLD_EFFT_D IS NOT NULL, {{ ref('Join_82') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join_82') }}
	WHERE {{ ref('Join_82') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtIntGrupEmplUpdateDS