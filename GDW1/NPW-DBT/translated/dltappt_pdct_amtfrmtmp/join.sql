{{ config(materialized='view', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

WITH Join AS (
	SELECT
		{{ ref('src_cpy') }}.APPT_PDCT_I,
		{{ ref('src_cpy') }}.AMT_TYPE_C,
		{{ ref('ChangeCapture') }}.CNCY_C,
		{{ ref('ChangeCapture') }}.APPT_PDCT_A,
		{{ ref('ChangeCapture') }}.XCES_AMT_REAS_X,
		{{ ref('src_cpy') }}.SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('src_cpy') }}.EROR_SEQN_I
	FROM {{ ref('ChangeCapture') }}
	RIGHT JOIN {{ ref('src_cpy') }} ON {{ ref('ChangeCapture') }}.APPT_PDCT_I = {{ ref('src_cpy') }}.APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.AMT_TYPE_C = {{ ref('src_cpy') }}.AMT_TYPE_C
	AND {{ ref('ChangeCapture') }}.SRCE_SYST_C = {{ ref('src_cpy') }}.SRCE_SYST_C
)

SELECT * FROM Join