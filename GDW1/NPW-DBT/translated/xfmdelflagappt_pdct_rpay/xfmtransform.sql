{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_RPAY']) }}

WITH XfmTransform AS (
	SELECT
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'HL' Else ( If InModNullHandling.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'PL' Else ''),
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'HL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'PL', '')) AS svStreamType,
		-- *SRC*: "CSE" : svStreamType : InModNullHandling.APP_PROD_ID,
		CONCAT(CONCAT('CSE', svStreamType), {{ ref('LkpReferences') }}.APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM XfmTransform