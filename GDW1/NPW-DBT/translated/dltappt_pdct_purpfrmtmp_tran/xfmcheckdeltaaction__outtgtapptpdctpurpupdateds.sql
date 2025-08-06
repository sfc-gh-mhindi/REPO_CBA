{{ config(materialized='view', tags=['DltAppt_Pdct_PurpfrmTMP_tran']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctPurpUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctPurp.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('RmvdupFrmJoin') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('RmvdupFrmJoin') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		{{ ref('RmvdupFrmJoin') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('RmvdupFrmJoin') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('RmvdupFrmJoin') }}
	WHERE {{ ref('RmvdupFrmJoin') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctPurpUpdateDS