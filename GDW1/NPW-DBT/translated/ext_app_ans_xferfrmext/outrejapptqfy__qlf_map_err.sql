{{ config(materialized='view', tags=['Ext_APP_ANS_XFERFrmExt']) }}

WITH OutRejApptQfy__qlf_map_err AS (
	SELECT
		{{ ref('LkpReferences') }}.HL_APP_ID AS SRCE_KEY_I,
		'SBTY_CODE ERROR' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'TYPE_APPT_QLFY' AS TRSF_TABL_M,
		{{ ref('LkpReferences') }}.SBTY_CODE AS VALU_CHNG_BFOR_X,
		{{ ref('LkpReferences') }}.SBTY_CODE AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'ACCT_QLFY_C' AS TRSF_COLM_M
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM OutRejApptQfy__qlf_map_err