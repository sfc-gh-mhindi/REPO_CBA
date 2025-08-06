{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH FrmCpyandCCpture AS (
	SELECT
		{{ ref('Chng_Cptr') }}.APPT_I,
		{{ ref('Chng_Cptr') }}.QSTN_C,
		{{ ref('Chng_Cptr') }}.RESP_C,
		{{ ref('Chng_Cptr') }}.RESP_CMMT_X,
		{{ ref('Chng_Cptr') }}.PATY_I,
		{{ ref('Chng_Cptr') }}.change_code,
		{{ ref('src_cpy') }}.ROW_SECU_ACCS_C,
		{{ ref('src_cpy') }}.EROR_SEQN_I
	FROM {{ ref('Chng_Cptr') }}
	LEFT JOIN {{ ref('src_cpy') }} ON {{ ref('Chng_Cptr') }}.APPT_I = {{ ref('src_cpy') }}.APPT_I
	AND {{ ref('Chng_Cptr') }}.QSTN_C = {{ ref('src_cpy') }}.QSTN_C
)

SELECT * FROM FrmCpyandCCpture