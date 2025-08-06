{{ config(materialized='view', tags=['DltAPPT_FEATFrmTMP_APPT_FEAT']) }}

WITH CpyApptFeat AS (
	SELECT
		NEW_APPT_I,
		NEW_FEAT_I,
		NEW_SRCE_SYST_C,
		NEW_SRCE_SYST_APPT_FEAT_I,
		NEW_SRCE_SYST_STND_VALU_Q,
		NEW_SRCE_SYST_STND_VALU_R,
		NEW_SRCE_SYST_STND_VALU_A,
		NEW_ACTL_VALU_Q,
		NEW_ACTL_VALU_R,
		NEW_ACTL_VALU_A,
		NEW_CNCY_C,
		NEW_OVRD_FEAT_I,
		NEW_OVRD_REAS_C,
		NEW_FEAT_SEQN_N,
		NEW_FEAT_STRT_D,
		NEW_FEE_CHRG_D,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_SRCE_SYST_APPT_FEAT_I AS NEW_SRCE_SYST_APPT_FEAT_I,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_SRCE_SYST_STND_VALU_R AS NEW_SRCE_SYST_STND_VALU_R,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_SRCE_SYST_STND_VALU_A AS NEW_SRCE_SYST_STND_VALU_A,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_ACTL_VALU_Q AS NEW_ACTL_VALU_Q,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_ACTL_VALU_R AS NEW_ACTL_VALU_R,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_ACTL_VALU_A AS NEW_ACTL_VALU_A,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_FEE_CHRG_D AS NEW_FEE_CHRG_D,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_FEAT_I AS NEW_FEAT_I,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_SRCE_SYST_C AS NEW_SRCE_SYST_C,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_SRCE_SYST_STND_VALU_Q AS NEW_SRCE_SYST_STND_VALU_Q,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_CNCY_C AS NEW_CNCY_C,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_OVRD_FEAT_I AS NEW_OVRD_FEAT_I,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_OVRD_REAS_C AS NEW_OVRD_REAS_C,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_FEAT_SEQN_N AS NEW_FEAT_SEQN_N,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_FEAT_STRT_D AS NEW_FEAT_STRT_D,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptFeatTera') }}
)

SELECT * FROM CpyApptFeat