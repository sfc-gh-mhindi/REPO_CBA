{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapCmpeID AS (
	SELECT
		{{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID AS SRCE_KEY_I,
		'FUNDING_NONCBA_INSTITUTION_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_CMPE_INDD' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.FUNDING_NONCBA_BANK_NUMBER AS VALU_CHNG_BFOR_X,
		'UNKN' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'CMPE_I' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE {{ ref('ModNullHandling') }}.FUNDING_NONCBA_BANK_NUMBER IS NOT NULL AND {{ ref('ModNullHandling') }}.CMPE_I = 'UNKN'
)

SELECT * FROM XfmBusinessRules__OutErrorMapCmpeID