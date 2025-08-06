{{ config(materialized='view', tags=['DltAPPT_PDCT_FEATFrmTMP_APPT_PDCT_FEAT_3']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_FEAT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_FEAT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_OVRD_I,
		{{ ref('ChangeCapture') }}.NEW_OVRD_FEAT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_STND_VALU_Q,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_STND_VALU_R,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_STND_VALU_A,
		{{ ref('ChangeCapture') }}.NEW_CNCY_C,
		{{ ref('ChangeCapture') }}.NEW_ACTL_VALU_Q,
		{{ ref('ChangeCapture') }}.NEW_ACTL_VALU_R,
		{{ ref('ChangeCapture') }}.NEW_ACTL_VALU_A,
		{{ ref('ChangeCapture') }}.NEW_FEAT_SEQN_N,
		{{ ref('ChangeCapture') }}.NEW_FEAT_STRT_D,
		{{ ref('ChangeCapture') }}.NEW_FEE_CHRG_D,
		{{ ref('ChangeCapture') }}.NEW_OVRD_REAS_C,
		{{ ref('ChangeCapture') }}.NEW_FEE_ADD_TO_TOTL_F,
		{{ ref('ChangeCapture') }}.NEW_FEE_CAPL_F,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptPdctFeat') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctFeat') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctFeat') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_FEAT_I = {{ ref('CpyApptPdctFeat') }}.NEW_SRCE_SYST_APPT_FEAT_I
)

SELECT * FROM JoinAll