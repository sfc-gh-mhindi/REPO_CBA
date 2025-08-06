{{ config(materialized='view', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

WITH XfmCheckDeltaAction__OutTgtApptEmplUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.OLD_EMPL_I)) THEN (JoinAllApptEmpl.OLD_EMPL_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EMPL_I IS NOT NULL, {{ ref('JoinAll') }}.OLD_EMPL_I, '') AS EMPL_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.NEW_APPT_I)) THEN (JoinAllApptEmpl.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.NEW_EMPL_ROLE_C)) THEN (JoinAllApptEmpl.NEW_EMPL_ROLE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_EMPL_ROLE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_EMPL_ROLE_C, '') AS EMPL_ROLE_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.OLD_EFFT_D)) THEN (JoinAllApptEmpl.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptEmplUpdateDS