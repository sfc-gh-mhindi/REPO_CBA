{{ config(materialized='view', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtEvntEmplUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((DSLink84.OLD_EVNT_I)) THEN (DSLink84.OLD_EVNT_I) ELSE ""),
		IFF({{ ref('Join_82') }}.OLD_EVNT_I IS NOT NULL, {{ ref('Join_82') }}.OLD_EVNT_I, '') AS EVNT_I,
		-- *SRC*: ( IF IsNotNull((DSLink84.OLD_EMPL_I)) THEN (DSLink84.OLD_EMPL_I) ELSE ""),
		IFF({{ ref('Join_82') }}.OLD_EMPL_I IS NOT NULL, {{ ref('Join_82') }}.OLD_EMPL_I, '') AS EMPL_I,
		-- *SRC*: ( IF IsNotNull((DSLink84.OLD_EFFT_D)) THEN (DSLink84.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('Join_82') }}.OLD_EFFT_D IS NOT NULL, {{ ref('Join_82') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join_82') }}
	WHERE {{ ref('Join_82') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtEvntEmplUpdateDS