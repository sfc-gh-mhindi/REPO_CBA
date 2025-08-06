{{ config(materialized='view', tags=['XfmDelFlagAPPT_REL']) }}

WITH XfmTransform AS (
	SELECT
		-- *SRC*: 'CSE' : 'CL' : InModNullHandling.DELETED_KEY_1_VALUE,
		CONCAT(CONCAT('CSE', 'CL'), {{ ref('LkpReferences') }}.DELETED_KEY_1_VALUE) AS APPT_I,
		-- *SRC*: 'CSE' : 'HL' : InModNullHandling.DELETED_KEY_2_VALUE,
		CONCAT(CONCAT('CSE', 'HL'), {{ ref('LkpReferences') }}.DELETED_KEY_2_VALUE) AS RELD_APPT_I,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_HL_APP' Then 'CLHL' Else ( If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_LINK' Then 'CCRA' Else ''),
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_HL_APP', 'CLHL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_LINK', 'CCRA', '')) AS REL_TYPE_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM XfmTransform