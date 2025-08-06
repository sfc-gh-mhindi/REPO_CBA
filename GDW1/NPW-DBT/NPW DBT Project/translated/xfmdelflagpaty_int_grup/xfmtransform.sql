{{ config(materialized='view', tags=['XfmDelFlagPATY_INT_GRUP']) }}

WITH XfmTransform AS (
	SELECT
		-- *SRC*: "CSEC1" : InModNullHandling.DELETED_KEY_2_VALUE,
		CONCAT('CSEC1', {{ ref('LkpReferences') }}.DELETED_KEY_2_VALUE) AS INT_GRUP_I,
		{{ ref('LkpReferences') }}.APP_PROD_ID AS SRCE_SYST_PATY_INT_GRUP_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM XfmTransform