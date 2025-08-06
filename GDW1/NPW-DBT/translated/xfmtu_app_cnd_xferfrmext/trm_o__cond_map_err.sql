{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH Trm_O__cond_Map_err AS (
	SELECT
		-- *SRC*: \(20)If IsNull(valid.COND_C) Then '9999' Else valid.COND_C,
		IFF({{ ref('LkpReferences') }}.COND_C IS NULL, '9999', {{ ref('LkpReferences') }}.COND_C) AS COND,
		{{ ref('LkpReferences') }}.HL_APP_PROD_ID AS SRCE_KEY_I,
		'COND_C' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_APPT_COND' AS TRSF_TABL_M,
		{{ ref('LkpReferences') }}.COND_C AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'APPT_COND_C' AS TRSF_COLM_M
	FROM {{ ref('LkpReferences') }}
	WHERE COND = '9999'
)

SELECT * FROM Trm_O__cond_Map_err