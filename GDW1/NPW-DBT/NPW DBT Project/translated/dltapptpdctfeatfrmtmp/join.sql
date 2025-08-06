{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH Join AS (
	SELECT
		{{ ref('src_cpy') }}.APPT_PDCT_I,
		{{ ref('src_cpy') }}.FEAT_I,
		{{ ref('src_cpy') }}.SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.ACTL_VALU_Q,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('src_cpy') }}.SRCE_SYST_APPT_FEAT_I,
		{{ ref('src_cpy') }}.SRCE_SYST_APPT_OVRD_I,
		{{ ref('src_cpy') }}.OVRD_FEAT_I,
		{{ ref('src_cpy') }}.SRCE_SYST_STND_VALU_Q,
		{{ ref('src_cpy') }}.SRCE_SYST_STND_VALU_R,
		{{ ref('src_cpy') }}.SRCE_SYST_STND_VALU_A,
		{{ ref('src_cpy') }}.CNCY_C,
		{{ ref('src_cpy') }}.ACTL_VALU_R,
		{{ ref('src_cpy') }}.ACTL_VALU_A,
		{{ ref('src_cpy') }}.FEAT_SEQN_N,
		{{ ref('src_cpy') }}.FEAT_STRT_D,
		{{ ref('src_cpy') }}.FEE_CHRG_D,
		{{ ref('src_cpy') }}.OVRD_REAS_C,
		{{ ref('src_cpy') }}.FEE_ADD_TO_TOTL_F,
		{{ ref('src_cpy') }}.FEE_CAPL_F,
		{{ ref('src_cpy') }}.EROR_SEQN_I
	FROM {{ ref('src_cpy') }}
	LEFT JOIN {{ ref('ChangeCapture') }} ON {{ ref('src_cpy') }}.APPT_PDCT_I = {{ ref('ChangeCapture') }}.APPT_PDCT_I
	AND {{ ref('src_cpy') }}.FEAT_I = {{ ref('ChangeCapture') }}.FEAT_I
	AND {{ ref('src_cpy') }}.SRCE_SYST_C = {{ ref('ChangeCapture') }}.SRCE_SYST_C
)

SELECT * FROM Join