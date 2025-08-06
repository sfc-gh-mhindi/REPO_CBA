{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_UNID_PATY']) }}

WITH XfmTransform AS (
	SELECT
		-- *SRC*: 'CSE' : 'PL' : InModNullHandling.DELETED_KEY_1_VALUE,
		CONCAT(CONCAT('CSE', 'PL'), {{ ref('LkpReferences') }}.DELETED_KEY_1_VALUE) AS APPT_PDCT_I,
		'TPBR' AS PATY_ROLE_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM XfmTransform