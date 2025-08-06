{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCT_FrmTMP_APPT_PDCT_ACCT']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctAcctUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_PDCT_I)) THEN (OutJoin.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		{{ ref('Join') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctAcctUpdateDS