{{ config(materialized='view', tags=['XfmDelFlagAPPT_FEAT']) }}

WITH XfmTransform AS (
	SELECT
		-- *SRC*: 'CSE' : 'CL' : InModNullHandling.DELETED_KEY_2_VALUE,
		CONCAT(CONCAT('CSE', 'CL'), {{ ref('LkpReferences') }}.DELETED_KEY_2_VALUE) AS APPT_I,
		-- *SRC*: 'CF' : InModNullHandling.DELETED_KEY_1_VALUE,
		CONCAT('CF', {{ ref('LkpReferences') }}.DELETED_KEY_1_VALUE) AS SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM XfmTransform