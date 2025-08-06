{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptPdctPatyDS AS (
	SELECT
		-- *SRC*: InXfmBusinessRules.APPT_QLFY_C = "99",
		{{ ref('ModNullHandling') }}.APPT_QLFY_C = '99' AS svApptQlfyNotFound,
		-- *SRC*: InXfmBusinessRules.ROLE_CAT_ID = "9999",
		{{ ref('ModNullHandling') }}.ROLE_CAT_ID = '9999' AS svPatyRoleCNotFound,
		-- *SRC*: Len(Trim(( IF IsNotNull((InXfmBusinessRules.CIF_CODE)) THEN (InXfmBusinessRules.CIF_CODE) ELSE ""))) = 0,
		LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CIF_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.CIF_CODE, ''))) = 0 AS svCifCodeIsNull,
		-- *SRC*: \(20)If svPatyRoleCNotFound Then "RPR5110" Else  If svApptQlfyNotFound Then "RPR5111" Else "",
		IFF(svPatyRoleCNotFound, 'RPR5110', IFF(svApptQlfyNotFound, 'RPR5111', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: "CSE" : InXfmBusinessRules.APPT_QLFY_C : ( IF IsNotNull((InXfmBusinessRules.APP_PROD_ID)) THEN (InXfmBusinessRules.APP_PROD_ID) ELSE ""),
		CONCAT(CONCAT('CSE', {{ ref('ModNullHandling') }}.APPT_QLFY_C), IFF({{ ref('ModNullHandling') }}.APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.APP_PROD_ID, '')) AS APPT_PDCT_I,
		-- *SRC*: "CIFPT+" : Str("0", 10 - Len(( IF IsNotNull((InXfmBusinessRules.CIF_CODE)) THEN (InXfmBusinessRules.CIF_CODE) ELSE ""))) : ( IF IsNotNull((InXfmBusinessRules.CIF_CODE)) THEN (InXfmBusinessRules.CIF_CODE) ELSE ""),
		CONCAT(CONCAT('CIFPT+', STR('0', 10 - LEN(IFF({{ ref('ModNullHandling') }}.CIF_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.CIF_CODE, '')))), IFF({{ ref('ModNullHandling') }}.CIF_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.CIF_CODE, '')) AS PATY_I,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.ROLE_CAT_ID)) THEN (InXfmBusinessRules.ROLE_CAT_ID) ELSE ""))) = 0 Then 'UNKN' Else InXfmBusinessRules.PATY_ROLE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ROLE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.ROLE_CAT_ID, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PATY_ROLE_C) AS PATY_ROLE_C,
		-- *SRC*: StringToDate(InXfmBusinessRules.ORIG_ETL_D, '%yyyy%mm%dd'),
		STRINGTODATE({{ ref('ModNullHandling') }}.ORIG_ETL_D, '%yyyy%mm%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		{{ ref('ModNullHandling') }}.APP_PROD_CLIENT_ROLE_ID AS SRCE_SYST_APPT_PDCT_PATY_I,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE Not svApptQlfyNotFound AND Not svCifCodeIsNull
)

SELECT * FROM XfmBusinessRules__OutTmpApptPdctPatyDS