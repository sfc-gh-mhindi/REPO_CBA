{{ config(materialized='view', tags=['DltAPPT_DOCU_DELY_INSSFrmTMP_APPT_DOCU_DELY_INSS']) }}

WITH XfmCheckDeltaAction__OutTgtApptDocuDelyInssUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('JoinAll') }}.NEW_APPT_I AS APPT_I,
		{{ ref('JoinAll') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptDocuDelyInssUpdateDS