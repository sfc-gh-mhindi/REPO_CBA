{{ config(materialized='view', tags=['DltAPPT_PDCT_FNDD_INSSFrmTMP_APPT_PDCT_FNDD_INSS']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctFnddInssUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('JoinAll') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_I AS APPT_PDCT_FNDD_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFnddInss.NEW_APPT_PDCT_FNDD_METH_I)) THEN (JoinAllApptPdctFnddInss.NEW_APPT_PDCT_FNDD_METH_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_METH_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_FNDD_METH_I, '') AS APPT_PDCT_FNDD_METH_I,
		{{ ref('JoinAll') }}.OLD_FNDD_INSS_METH_C AS FNDD_INSS_METH_C,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctFnddInssUpdateDS