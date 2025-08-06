{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH XfmBusinessRules__OutErrorMAP_CSE_CRIS_PDCTSeq AS (
	SELECT
		-- *SRC*: \(20)if (len(trim(( IF IsNotNull((InModNullHandling.ACCT_QLFY_C)) THEN (InModNullHandling.ACCT_QLFY_C) ELSE ""))) <> 0 or len(trim(( IF IsNotNull((InModNullHandling.SRCE_SYST_C)) THEN (InModNullHandling.SRCE_SYST_C) ELSE ""))) <> 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.ACCT_QLFY_C IS NOT NULL, {{ ref('LkpReferences') }}.ACCT_QLFY_C, ''))) <> 0 OR LEN(TRIM(IFF({{ ref('LkpReferences') }}.SRCE_SYST_C IS NOT NULL, {{ ref('LkpReferences') }}.SRCE_SYST_C, ''))) <> 0, 'Y', 'N') AS LoadAdrs,
		{{ ref('LkpReferences') }}.HL_APP_PROD_ID AS SRCE_KEY_I,
		'APPLICATION_QUALIFY_CODE' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_CRIS_PDCT' AS TRSF_TABL_M,
		-- *SRC*: ( IF IsNotNull((InModNullHandling.ACCT_QLFY_C)) THEN (InModNullHandling.ACCT_QLFY_C) ELSE ""),
		IFF({{ ref('LkpReferences') }}.ACCT_QLFY_C IS NOT NULL, {{ ref('LkpReferences') }}.ACCT_QLFY_C, '') AS VALU_CHNG_BFOR_X,
		'' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'frmlkp.ACCT_QLFY_C' AS TRSF_COLM_M
	FROM {{ ref('LkpReferences') }}
	WHERE LoadAdrs = 'N'
)

SELECT * FROM XfmBusinessRules__OutErrorMAP_CSE_CRIS_PDCTSeq