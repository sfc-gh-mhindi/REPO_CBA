{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCTFrmTMP_APPT_PDCT_ACCT']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctAcctUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_PDCT_I)) THEN (OutJoin.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_ACCT_I)) THEN (OutJoin.OLD_ACCT_I) ELSE ""),
		IFF({{ ref('Join') }}.OLD_ACCT_I IS NOT NULL, {{ ref('Join') }}.OLD_ACCT_I, '') AS ACCT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_REL_TYPE_C)) THEN (OutJoin.NEW_REL_TYPE_C) ELSE ""),
		IFF({{ ref('Join') }}.NEW_REL_TYPE_C IS NOT NULL, {{ ref('Join') }}.NEW_REL_TYPE_C, '') AS REL_TYPE_C,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_EFFT_D)) THEN (OutJoin.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('Join') }}.OLD_EFFT_D IS NOT NULL, {{ ref('Join') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctAcctUpdateDS