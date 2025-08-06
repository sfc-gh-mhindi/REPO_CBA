{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_AMT']) }}

WITH XfmTransform__OutApptPdctDS3 AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.DUMMY_PDCT_F)) THEN (InModNullHandling.DUMMY_PDCT_F) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.DUMMY_PDCT_F IS NOT NULL, {{ ref('LkpReferences') }}.DUMMY_PDCT_F, ''))) = 0, 'Y', 'N') AS svIsExistAppProdId,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'Y', 'N') AS svLoadCclAppProd,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR', 'Y', 'N') AS svHlAppProdRecord,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'HL' Else  If InModNullHandling.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'PL' Else  If InModNullHandling.DELETED_TABLE_NAME = 'CC_APP_PROD' Then 'CC' Else  If InModNullHandling.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR' Then 'HL' Else '',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'HL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'PL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CC_APP_PROD', 'CC', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR', 'HL', '')))) AS svStreamType,
		-- *SRC*: "CSE" : svStreamType : InModNullHandling.APP_PROD_ID,
		CONCAT(CONCAT('CSE', svStreamType), {{ ref('LkpReferences') }}.APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE svHlAppProdRecord = 'Y'
)

SELECT * FROM XfmTransform__OutApptPdctDS3