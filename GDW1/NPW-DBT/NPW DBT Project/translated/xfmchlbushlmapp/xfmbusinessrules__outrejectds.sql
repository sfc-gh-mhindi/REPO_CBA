{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH XfmBusinessRules__Outrejectds AS (
	SELECT
		-- *SRC*: \(20)if (len(trim(( IF IsNotNull((InModNullHandling.ACCT_QLFY_C)) THEN (InModNullHandling.ACCT_QLFY_C) ELSE ""))) <> 0 or len(trim(( IF IsNotNull((InModNullHandling.SRCE_SYST_C)) THEN (InModNullHandling.SRCE_SYST_C) ELSE ""))) <> 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.ACCT_QLFY_C IS NOT NULL, {{ ref('LkpReferences') }}.ACCT_QLFY_C, ''))) <> 0 OR LEN(TRIM(IFF({{ ref('LkpReferences') }}.SRCE_SYST_C IS NOT NULL, {{ ref('LkpReferences') }}.SRCE_SYST_C, ''))) <> 0, 'Y', 'N') AS LoadAdrs,
		{{ ref('LkpReferences') }}.APP_ID AS HLM_APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		{{ ref('LkpReferences') }}.DCHG_REAS_ID AS DISCHARGE_REASON_ID,
		HL_APP_PROD_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		'RPR7501' AS EROR_C
	FROM {{ ref('LkpReferences') }}
	WHERE LoadAdrs = 'N'
)

SELECT * FROM XfmBusinessRules__Outrejectds