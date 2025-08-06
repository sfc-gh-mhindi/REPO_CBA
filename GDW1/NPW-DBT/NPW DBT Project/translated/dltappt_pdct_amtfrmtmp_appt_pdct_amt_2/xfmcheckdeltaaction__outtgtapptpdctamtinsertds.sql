{{ config(materialized='view', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_2']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctAmtInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_PDCT_I)) THEN (OutJoin.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_AMT_TYPE_C)) THEN (OutJoin.NEW_AMT_TYPE_C) ELSE ""),
		IFF({{ ref('Join') }}.NEW_AMT_TYPE_C IS NOT NULL, {{ ref('Join') }}.NEW_AMT_TYPE_C, '') AS AMT_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		{{ ref('Join') }}.NEW_CNCY_C AS CNCY_C,
		{{ ref('Join') }}.NEW_APPT_PDCT_A AS APPT_PDCT_A,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = INSERT OR {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctAmtInsertDS