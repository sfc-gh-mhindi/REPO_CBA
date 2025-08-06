{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT']) }}

WITH XfmTransform__OutApptPdctDS1 AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.DUMMY_PDCT_F)) THEN (InModNullHandling.DUMMY_PDCT_F) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.DUMMY_PDCT_F IS NOT NULL, {{ ref('LkpReferences') }}.DUMMY_PDCT_F, ''))) = 0, 'Y', 'N') AS svIsExistAppProdId,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'Y', 'N') AS svLoadCclAppProd,
		APPT_I,
		{{ ref('LkpReferences') }}.DLTD_KEY1_VALU AS APPT_PDCT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE svIsExistAppProdId = 'Y' AND svLoadCclAppProd = 'Y'
)

SELECT * FROM XfmTransform__OutApptPdctDS1