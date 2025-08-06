{{ config(materialized='view', tags=['DltAPPT_PDCT_PURP_rmTMP_APPT_PDCT_PURP']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctPurpInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctPurp.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_SRCE_SYST_APPT_PDCT_PURP_I)) THEN (JoinAllApptPdctPurp.NEW_SRCE_SYST_APPT_PDCT_PURP_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I, '') AS SRCE_SYST_APPT_PDCT_PURP_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_PURP_TYPE_C)) THEN (JoinAllApptPdctPurp.NEW_PURP_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_PURP_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_PURP_TYPE_C, '') AS PURP_TYPE_C,
		{{ ref('JoinAll') }}.NEW_PURP_CLAS_C AS PURP_CLAS_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctPurp.NEW_SRCE_SYST_C)) THEN (JoinAllApptPdctPurp.NEW_SRCE_SYST_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_C, '') AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_PURP_A AS PURP_A,
		{{ ref('JoinAll') }}.NEW_CNCY_C AS CNCY_C,
		{{ ref('JoinAll') }}.NEW_MAIN_PURP_F AS MAIN_PURP_F,
		'9999-12-31' AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctPurpInsertDS