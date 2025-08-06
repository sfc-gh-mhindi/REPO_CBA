{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapFnddInssMethC AS (
	SELECT
		-- *SRC*: ( IF IsNotNull((InXfmBusinessRules.TU_APP_FUNDING_DETAIL_ID)) THEN (InXfmBusinessRules.TU_APP_FUNDING_DETAIL_ID) ELSE ""),
		IFF({{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID IS NOT NULL, {{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID, '') AS SRCE_KEY_I,
		'FUNDING_METHOD_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_PACK_PDCT_HL' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.FUNDING_METHOD_CAT_ID AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'FNDD_INSS_METH_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE {{ ref('ModNullHandling') }}.FUNDING_METHOD_CAT_ID IS NOT NULL AND TRIM({{ ref('ModNullHandling') }}.FNDD_INSS_METH_C) = '9999'
)

SELECT * FROM XfmBusinessRules__OutErrorMapFnddInssMethC