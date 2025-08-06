{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapFnddD AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((InXfmBusinessRules.TU_APP_FUNDING_DETAIL_ID)) THEN (InXfmBusinessRules.TU_APP_FUNDING_DETAIL_ID) ELSE ""),
		IFF({{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID IS NOT NULL, {{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID, '') AS SRCE_KEY_I,
		'FUNDING_DATE' AS CONV_M,
		'INVALID' AS CONV_MAP_RULE_M,
		'COLUMN FUNDING_DATE' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.FUNDING_DATE AS VALU_CHNG_BFOR_X,
		'11111111' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'FNDD_D' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE LEN(TRIM(IFF({{ ref('ModNullHandling') }}.FUNDING_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.FUNDING_DATE, ''))) <> 8 AND {{ ref('ModNullHandling') }}.FUNDING_DATE IS NOT NULL
)

SELECT * FROM XfmBusinessRules__OutErrorMapFnddD