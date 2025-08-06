{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH Join AS (
	SELECT
		{{ ref('ChangeCapture') }}.APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.RPAY_SRCE_C,
		{{ ref('ChangeCapture') }}.RPAY_SRCE_OTHR_X,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('src_cpy') }}.RPAY_TYPE_C,
		{{ ref('src_cpy') }}.PAYT_FREQ_C,
		{{ ref('src_cpy') }}.STRT_RPAY_D,
		{{ ref('src_cpy') }}.EROR_SEQN_I
	FROM {{ ref('ChangeCapture') }}
	LEFT JOIN {{ ref('src_cpy') }} ON {{ ref('ChangeCapture') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
)

SELECT * FROM Join