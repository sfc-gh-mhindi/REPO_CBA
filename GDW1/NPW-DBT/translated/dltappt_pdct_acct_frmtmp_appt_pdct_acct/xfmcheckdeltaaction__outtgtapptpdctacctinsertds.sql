{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCT_FrmTMP_APPT_PDCT_ACCT']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctAcctInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_PDCT_I)) THEN (OutJoin.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_ACCT_I)) THEN (OutJoin.NEW_ACCT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_ACCT_I IS NOT NULL, {{ ref('Join') }}.NEW_ACCT_I, '') AS ACCT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_REL_TYPE_C)) THEN (OutJoin.NEW_REL_TYPE_C) ELSE ""),
		IFF({{ ref('Join') }}.NEW_REL_TYPE_C IS NOT NULL, {{ ref('Join') }}.NEW_REL_TYPE_C, '') AS REL_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = INSERT OR {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctAcctInsertDS