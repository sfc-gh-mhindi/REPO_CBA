{{ config(materialized='view', tags=['DltAPPT_FEATFrmTMP_APPT_FEAT']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_FEAT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_STND_VALU_R,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_STND_VALU_A,
		{{ ref('ChangeCapture') }}.NEW_ACTL_VALU_Q,
		{{ ref('ChangeCapture') }}.NEW_ACTL_VALU_R,
		{{ ref('ChangeCapture') }}.NEW_ACTL_VALU_A,
		{{ ref('ChangeCapture') }}.NEW_FEE_CHRG_D,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptFeat') }}.NEW_FEAT_I,
		{{ ref('CpyApptFeat') }}.NEW_SRCE_SYST_C,
		{{ ref('CpyApptFeat') }}.NEW_SRCE_SYST_STND_VALU_Q,
		{{ ref('CpyApptFeat') }}.NEW_CNCY_C,
		{{ ref('CpyApptFeat') }}.NEW_OVRD_FEAT_I,
		{{ ref('CpyApptFeat') }}.NEW_OVRD_REAS_C,
		{{ ref('CpyApptFeat') }}.NEW_FEAT_SEQN_N,
		{{ ref('CpyApptFeat') }}.NEW_FEAT_STRT_D,
		{{ ref('CpyApptFeat') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptFeat') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptFeat') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_FEAT_I = {{ ref('CpyApptFeat') }}.NEW_SRCE_SYST_APPT_FEAT_I
)

SELECT * FROM JoinAll