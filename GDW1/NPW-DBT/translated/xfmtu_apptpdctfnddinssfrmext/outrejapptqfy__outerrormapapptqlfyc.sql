{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH OutRejApptQfy__OutErrorMapApptQlfyC AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((OutAppPdctRejectsDS.TU_APP_FUNDING_DETAIL_ID)) THEN (OutAppPdctRejectsDS.TU_APP_FUNDING_DETAIL_ID) ELSE ""),
		IFF({{ ref('LkpReferences') }}.TU_APP_FUNDING_DETAIL_ID IS NOT NULL, {{ ref('LkpReferences') }}.TU_APP_FUNDING_DETAIL_ID, '') AS SRCE_KEY_I,
		'SBTY_CODE' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_QLFY' AS TRSF_TABL_M,
		-- *SRC*: ( IF IsNotNull((OutAppPdctRejectsDS.SBTY_CODE)) THEN (OutAppPdctRejectsDS.SBTY_CODE) ELSE ""),
		IFF({{ ref('LkpReferences') }}.SBTY_CODE IS NOT NULL, {{ ref('LkpReferences') }}.SBTY_CODE, '') AS VALU_CHNG_BFOR_X,
		'REJECTED' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'APPT_QLFY_C' AS TRSF_COLM_M
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM OutRejApptQfy__OutErrorMapApptQlfyC