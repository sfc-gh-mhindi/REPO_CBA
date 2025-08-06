{{ config(materialized='view', tags=['XfmDelFlagAPPT_TRNF_DETL']) }}

WITH XfmTransform AS (
	SELECT
		APPT_I,
		{{ ref('LkpReferences') }}.DELETED_KEY_1_VALUE AS APPT_TRNF_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM XfmTransform