{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH Transformer_32__toerrDateRoleC AS (
	SELECT
		-- *SRC*: \(20)If IsNull(totrans.CHNG_REAS_TYPE_C) OR Trim(( IF IsNotNull((totrans.CHNG_REAS_TYPE_C)) THEN (totrans.CHNG_REAS_TYPE_C) ELSE "")) = '' Then 'N' ELSE 'Y',
		IFF({{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C IS NULL OR TRIM(IFF({{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C IS NOT NULL, {{ ref('FunnelTrans') }}.CHNG_REAS_TYPE_C, '')) = '', 'N', 'Y') AS svChngReasType,
		-- *SRC*: \(20)IF IsNull(totrans.MODF_D) then 'N' Else  If (totrans.MODF_D = StringToDate(1111 : '-' : 11 : '-' : 11, "%yyyy-%mm-%dd")) THEN "Y" ELSE "N",
		IFF({{ ref('FunnelTrans') }}.MODF_D IS NULL, 'N', IFF({{ ref('FunnelTrans') }}.MODF_D = STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(1111, '-'), 11), '-'), 11), '%yyyy-%mm-%dd'), 'Y', 'N')) AS svIserrModfD,
		-- *SRC*: \(20)IF IsNull(totrans.MODF_T) then 'N' Else  If (totrans.MODF_T = '00:00:00') THEN "Y" ELSE "N",
		IFF({{ ref('FunnelTrans') }}.MODF_T IS NULL, 'N', IFF({{ ref('FunnelTrans') }}.MODF_T = '00:00:00', 'Y', 'N')) AS svIserrModfT,
		-- *SRC*: \(20)IF IsNull(totrans.GNRC_ROLE_S) then 'N' Else  If (totrans.GNRC_ROLE_S = '1111-11-11 00:00:00') THEN "Y" ELSE "N",
		IFF({{ ref('FunnelTrans') }}.GNRC_ROLE_S IS NULL, 'N', IFF({{ ref('FunnelTrans') }}.GNRC_ROLE_S = '1111-11-11 00:00:00', 'Y', 'N')) AS svIserrGnrcRoleS,
		{{ ref('FunnelTrans') }}.ccl_app_id AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'Lookup failure on MAP_CSE_APPT_GNRC_DATE_DR table' AS CONV_MAP_RULE_M,
		'APPT_GNRC_DATE' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		{{ ref('FunnelTrans') }}.promise_type AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DsjobName AS TRSF_X,
		'DATE_ROLE_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CCL_CLI_DATE_EXP_AUD' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: 'CSECL' : totrans.ccl_app_id,
		CONCAT('CSECL', {{ ref('FunnelTrans') }}.ccl_app_id) AS TRSF_KEY_I
	FROM {{ ref('FunnelTrans') }}
	WHERE {{ ref('FunnelTrans') }}.DATE_ROLE_C = '9999'
)

SELECT * FROM Transformer_32__toerrDateRoleC