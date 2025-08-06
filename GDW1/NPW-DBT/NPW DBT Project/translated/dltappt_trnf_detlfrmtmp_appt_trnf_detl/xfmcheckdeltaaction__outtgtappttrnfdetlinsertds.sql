{{ config(materialized='view', tags=['DltAPPT_TRNF_DETLFrmTMP_APPT_TRNF_DETL']) }}

WITH XfmCheckDeltaAction__OutTgtApptTrnfDetlInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_I)) THEN (OutJoin.NEW_APPT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_TRNF_I)) THEN (OutJoin.NEW_APPT_TRNF_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_TRNF_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_TRNF_I, '') AS APPT_TRNF_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('Join') }}.NEW_TRNF_OPTN_C AS TRNF_OPTN_C,
		{{ ref('Join') }}.NEW_TRNF_A AS TRNF_A,
		{{ ref('Join') }}.NEW_CNCY_C AS CNCY_C,
		{{ ref('Join') }}.NEW_CMPE_I AS CMPE_I,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = INSERT OR {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptTrnfDetlInsertDS