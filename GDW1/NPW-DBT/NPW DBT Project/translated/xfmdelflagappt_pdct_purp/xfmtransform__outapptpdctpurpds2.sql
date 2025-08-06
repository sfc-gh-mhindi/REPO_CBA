{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_PURP']) }}

WITH XfmTransform__OutApptPdctPurpDS2 AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.DUMMY_PDCT_F)) THEN (InModNullHandling.DUMMY_PDCT_F) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.DUMMY_PDCT_F IS NOT NULL, {{ ref('LkpReferences') }}.DUMMY_PDCT_F, ''))) = 0, 'Y', 'N') AS svIsExistAppProdId,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'Y', 'N') AS svLoadCclAppProd,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'HL_APP_PROD_PURPOSE' Then 'HL' Else ( If InModNullHandling.DELETED_TABLE_NAME = 'PL_APP_PROD_PURP' Then 'PL' Else ''),
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_APP_PROD_PURPOSE', 'HL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_APP_PROD_PURP', 'PL', '')) AS svStreamType,
		-- *SRC*: "CSE" : svStreamType : InModNullHandling.DELETED_KEY_2_VALUE,
		CONCAT(CONCAT('CSE', svStreamType), {{ ref('LkpReferences') }}.DELETED_KEY_2_VALUE) AS APPT_PDCT_I,
		{{ ref('LkpReferences') }}.DELETED_KEY_1_VALUE AS SRCE_SYST_APPT_PDCT_PURP_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE svLoadCclAppProd = 'N'
)

SELECT * FROM XfmTransform__OutApptPdctPurpDS2