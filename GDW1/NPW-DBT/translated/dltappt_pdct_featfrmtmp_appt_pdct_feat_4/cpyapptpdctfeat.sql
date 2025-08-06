{{ config(materialized='view', tags=['DltAPPT_PDCT_FEATFrmTMP_APPT_PDCT_FEAT_4']) }}

WITH CpyApptPdctFeat AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_FEAT_I,
		NEW_SRCE_SYST_APPT_FEAT_I,
		NEW_SRCE_SYST_C,
		NEW_SRCE_SYST_APPT_OVRD_I,
		NEW_OVRD_FEAT_I,
		NEW_SRCE_SYST_STND_VALU_Q,
		NEW_SRCE_SYST_STND_VALU_R,
		NEW_SRCE_SYST_STND_VALU_A,
		NEW_CNCY_C,
		NEW_ACTL_VALU_Q,
		NEW_ACTL_VALU_R,
		NEW_ACTL_VALU_A,
		NEW_FEAT_SEQN_N,
		NEW_FEAT_STRT_D,
		NEW_FEE_CHRG_D,
		NEW_OVRD_REAS_C,
		NEW_FEE_ADD_TO_TOTL_F,
		NEW_FEE_CAPL_F,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_FEAT_I AS NEW_FEAT_I,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_ACTL_VALU_Q AS NEW_ACTL_VALU_Q,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_SRCE_SYST_APPT_FEAT_I AS NEW_SRCE_SYST_APPT_FEAT_I,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_SRCE_SYST_C AS NEW_SRCE_SYST_C,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_SRCE_SYST_APPT_OVRD_I AS NEW_SRCE_SYST_APPT_OVRD_I,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_OVRD_FEAT_I AS NEW_OVRD_FEAT_I,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_SRCE_SYST_STND_VALU_Q AS NEW_SRCE_SYST_STND_VALU_Q,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_SRCE_SYST_STND_VALU_R AS NEW_SRCE_SYST_STND_VALU_R,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_SRCE_SYST_STND_VALU_A AS NEW_SRCE_SYST_STND_VALU_A,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_CNCY_C AS NEW_CNCY_C,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_ACTL_VALU_R AS NEW_ACTL_VALU_R,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_ACTL_VALU_A AS NEW_ACTL_VALU_A,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_FEAT_SEQN_N AS NEW_FEAT_SEQN_N,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_FEAT_STRT_D AS NEW_FEAT_STRT_D,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_FEE_CHRG_D AS NEW_FEE_CHRG_D,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_OVRD_REAS_C AS NEW_OVRD_REAS_C,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_FEE_ADD_TO_TOTL_F AS NEW_FEE_ADD_TO_TOTL_F,
		{{ ref('SrcTmpApptPdctFeatTera') }}.OLD_FEE_CAPL_F AS NEW_FEE_CAPL_F,
		OLD_FEAT_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctFeatTera') }}
)

SELECT * FROM CpyApptPdctFeat