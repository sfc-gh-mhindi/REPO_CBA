{{ config(materialized='view', tags=['DltAPPT_PDCT_FNDD_INSSFrmTMP_APPT_PDCT_FNDD_INSS']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctFnddInssInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctFnddInss.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_APPT_PDCT_FNDD_I)) THEN (JoinAllApptPdctFnddInss.NEW_APPT_PDCT_FNDD_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_I, '') AS APPT_PDCT_FNDD_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_APPT_PDCT_FNDD_METH_I)) THEN (JoinAllApptPdctFnddInss.NEW_APPT_PDCT_FNDD_METH_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_METH_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_METH_I, '') AS APPT_PDCT_FNDD_METH_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_FNDD_INSS_METH_C)) THEN (JoinAllApptPdctFnddInss.NEW_FNDD_INSS_METH_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_FNDD_INSS_METH_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_FNDD_INSS_METH_C, '') AS FNDD_INSS_METH_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_SRCE_SYST_FNDD_I)) THEN (JoinAllApptPdctFnddInss.NEW_SRCE_SYST_FNDD_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_FNDD_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_FNDD_I, '') AS SRCE_SYST_FNDD_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_SRCE_SYST_FNDD_METH_I)) THEN (JoinAllApptPdctFnddInss.NEW_SRCE_SYST_FNDD_METH_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_FNDD_METH_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_FNDD_METH_I, '') AS SRCE_SYST_FNDD_METH_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_SRCE_SYST_C)) THEN (JoinAllApptPdctFnddInss.NEW_SRCE_SYST_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SRCE_SYST_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_SRCE_SYST_C, '') AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_FNDD_D AS FNDD_D,
		{{ ref('JoinAll') }}.NEW_FNDD_A AS FNDD_A,
		{{ ref('JoinAll') }}.NEW_PDCT_SYST_ACCT_N AS PDCT_SYST_ACCT_N,
		{{ ref('JoinAll') }}.NEW_CMPE_I AS CMPE_I,
		{{ ref('JoinAll') }}.NEW_CMPE_ACCT_BSB_N AS CMPE_ACCT_BSB_N,
		{{ ref('JoinAll') }}.NEW_CMPE_ACCT_N AS CMPE_ACCT_N,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctFnddInssInsertDS