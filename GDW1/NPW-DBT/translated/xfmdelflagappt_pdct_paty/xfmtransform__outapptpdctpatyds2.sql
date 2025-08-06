{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_PATY']) }}

WITH XfmTransform__OutApptPdctPatyDS2 AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.DUMMY_PDCT_F)) THEN (InModNullHandling.DUMMY_PDCT_F) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.DUMMY_PDCT_F IS NOT NULL, {{ ref('LkpReferences') }}.DUMMY_PDCT_F, ''))) = 0, 'Y', 'N') AS svIsExistAppProdId,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.APPT_QLFY_C)) THEN (InModNullHandling.APPT_QLFY_C) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.APPT_QLFY_C IS NOT NULL, {{ ref('LkpReferences') }}.APPT_QLFY_C, ''))) = 0, 'N', 'Y') AS svApptQlfyC,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.PATY_ROLE_C)) THEN (InModNullHandling.PATY_ROLE_C) ELSE ""))) = 0) Then '9999' Else InModNullHandling.PATY_ROLE_C,
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.PATY_ROLE_C IS NOT NULL, {{ ref('LkpReferences') }}.PATY_ROLE_C, ''))) = 0, '9999', {{ ref('LkpReferences') }}.PATY_ROLE_C) AS svPatyRoleC,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'APP_PROD_CLIENT_ROLE' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'APP_PROD_CLIENT_ROLE', 'Y', 'N') AS svLoadClientRole,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'Y', 'N') AS svLoadAppProd,
		-- *SRC*: "CSE" : "CL" : InModNullHandling.APP_PROD_ID,
		CONCAT(CONCAT('CSE', 'CL'), {{ ref('LkpReferences') }}.APP_PROD_ID) AS APPT_PDCT_I,
		'TPBR' AS PATY_ROLE_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE svIsExistAppProdId = 'Y' AND svLoadAppProd = 'Y'
)

SELECT * FROM XfmTransform__OutApptPdctPatyDS2