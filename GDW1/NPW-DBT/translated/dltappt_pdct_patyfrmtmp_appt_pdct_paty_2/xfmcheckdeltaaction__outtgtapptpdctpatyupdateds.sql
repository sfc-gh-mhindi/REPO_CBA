{{ config(materialized='view', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY_2']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctPatyUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctpaty.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.OLD_PATY_I)) THEN (JoinAllApptPdctpaty.OLD_PATY_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_PATY_I IS NOT NULL, {{ ref('JoinAll') }}.OLD_PATY_I, '') AS PATY_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.NEW_PATY_ROLE_C)) THEN (JoinAllApptPdctpaty.NEW_PATY_ROLE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_PATY_ROLE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_PATY_ROLE_C, '') AS PATY_ROLE_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.OLD_EFFT_D)) THEN (JoinAllApptPdctpaty.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		REFR_PK AS PROS_KEY_EXPY_I,
		ExpiryDate AS EXPY_D
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctPatyUpdateDS