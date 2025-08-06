{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH OutRejApptQfy__OutErrorMapPdctN AS (
	SELECT
		{{ ref('LkpReferences') }}.TU_APP_ID AS SRCE_KEY_I,
		'SBTY_CODE' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'CSE_APPT_QLFY' AS TRSF_TABL_M,
		{{ ref('LkpReferences') }}.SBTY_CODE AS VALU_CHNG_BFOR_X,
		{{ ref('LkpReferences') }}.SBTY_CODE AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'ACCT_QLFY_C' AS TRSF_COLM_M
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM OutRejApptQfy__OutErrorMapPdctN