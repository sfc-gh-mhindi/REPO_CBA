{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH Transformer_243__OutErrRespC AS (
	SELECT
		-- *SRC*: \(20)If IsNull(DSLink244.QSTN_C) Then '9999' Else DSLink244.QSTN_C,
		IFF({{ ref('Lookup_239') }}.QSTN_C IS NULL, '9999', {{ ref('Lookup_239') }}.QSTN_C) AS svQstnchk,
		-- *SRC*: \(20)If IsNull(DSLink244.QSTN_C_EXT) Then '9999' Else DSLink244.QSTN_C_EXT,
		IFF({{ ref('Lookup_239') }}.QSTN_C_EXT IS NULL, '9999', {{ ref('Lookup_239') }}.QSTN_C_EXT) AS svRespchk,
		-- *SRC*: \(20)If Trim(( IF IsNotNull((DSLink244.ANS_ID)) THEN (DSLink244.ANS_ID) ELSE "")) = '' Or DSLink244.ANS_ID = '0' Then svRespchk Else  If IsNull(DSLink244.RESP_C) Then '9999' Else DSLink244.RESP_C,
		IFF(TRIM(IFF({{ ref('Lookup_239') }}.ANS_ID IS NOT NULL, {{ ref('Lookup_239') }}.ANS_ID, '')) = '' OR {{ ref('Lookup_239') }}.ANS_ID = '0', svRespchk, IFF({{ ref('Lookup_239') }}.RESP_C IS NULL, '9999', {{ ref('Lookup_239') }}.RESP_C)) AS svREspchk1,
		{{ ref('Lookup_239') }}.APPT_I AS SRCE_KEY_I,
		'QA_ANSWER_ID' AS CONV_M,
		'' AS CONV_MAP_RULE_M,
		'APPT_QSTN' AS TRSF_TABL_M,
		{{ ref('Lookup_239') }}.ANS_ID AS VALU_CHNG_BFOR_X,
		svREspchk1 AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name=' : DSJobName,
		CONCAT('Job Name=', DSJobName) AS TRSF_X,
		'RESP_C' AS TRSF_COLM_M
	FROM {{ ref('Lookup_239') }}
	WHERE svREspchk1 = '9999'
)

SELECT * FROM Transformer_243__OutErrRespC