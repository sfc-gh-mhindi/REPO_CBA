{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH XfmBusinessRules__OutBusApPrdClntRejectsDS AS (
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
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag
)

SELECT * FROM XfmBusinessRules__OutBusApPrdClntRejectsDS