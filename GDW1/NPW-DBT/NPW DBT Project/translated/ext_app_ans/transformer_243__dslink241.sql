{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH Transformer_243__DSLink241 AS (
	SELECT
		-- *SRC*: \(20)If IsNull(DSLink244.QSTN_C) Then '9999' Else DSLink244.QSTN_C,
		IFF({{ ref('Lookup_239') }}.QSTN_C IS NULL, '9999', {{ ref('Lookup_239') }}.QSTN_C) AS svQstnchk,
		-- *SRC*: \(20)If IsNull(DSLink244.QSTN_C_EXT) Then '9999' Else DSLink244.QSTN_C_EXT,
		IFF({{ ref('Lookup_239') }}.QSTN_C_EXT IS NULL, '9999', {{ ref('Lookup_239') }}.QSTN_C_EXT) AS svRespchk,
		-- *SRC*: \(20)If Trim(( IF IsNotNull((DSLink244.ANS_ID)) THEN (DSLink244.ANS_ID) ELSE "")) = '' Or DSLink244.ANS_ID = '0' Then svRespchk Else  If IsNull(DSLink244.RESP_C) Then '9999' Else DSLink244.RESP_C,
		IFF(TRIM(IFF({{ ref('Lookup_239') }}.ANS_ID IS NOT NULL, {{ ref('Lookup_239') }}.ANS_ID, '')) = '' OR {{ ref('Lookup_239') }}.ANS_ID = '0', svRespchk, IFF({{ ref('Lookup_239') }}.RESP_C IS NULL, '9999', {{ ref('Lookup_239') }}.RESP_C)) AS svREspchk1,
		APPT_I,
		svQstnchk AS QSTN_C,
		EFFT_D,
		svREspchk1 AS RESP_C,
		RESP_CMMT_X,
		EXPY_D,
		PATY_I,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM {{ ref('Lookup_239') }}
	WHERE 
)

SELECT * FROM Transformer_243__DSLink241