{{ config(materialized='view', tags=['DltAPPT_PDCT_PURPrmTMP_APPT_PDCT_PURP']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctPurpUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctPurp.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.OLD_EFFT_D)) THEN (JoinAllApptPdctPurp.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_SRCE_SYST_APPT_PDCT_PURP_I)) THEN (JoinAllApptPdctPurp.NEW_SRCE_SYST_APPT_PDCT_PURP_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I, '') AS SRCE_SYST_APPT_PDCT_PURP_I,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctPurpUpdateDS