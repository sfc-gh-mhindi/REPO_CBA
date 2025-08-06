{{ config(materialized='view', tags=['DltAPPT_PDCT_RPAYFrmTMP_APPT_PDCT_RPAY']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctRpayInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFeat.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctFeat.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFeat.NEW_RPAY_TYPE_C)) THEN (JoinAllApptPdctFeat.NEW_RPAY_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_RPAY_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_RPAY_TYPE_C, '') AS RPAY_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('JoinAll') }}.NEW_PAYT_FREQ_C AS PAYT_FREQ_C,
		{{ ref('JoinAll') }}.NEW_STRT_RPAY_D AS STRT_RPAY_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: Setnull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctRpayInsertDS