{{ config(materialized='view', tags=['DltAPPT_PDCTFrmTMP_APPT_PDCT']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdct.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.OLD_APPT_I)) THEN (JoinAllApptPdct.OLD_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.OLD_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdct.OLD_EFFT_D)) THEN (JoinAllApptPdct.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctUpdateDS