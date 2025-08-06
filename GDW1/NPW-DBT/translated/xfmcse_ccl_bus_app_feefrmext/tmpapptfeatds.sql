{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__ccl__bus__app__fee__appt__feat', incremental_strategy='insert_overwrite', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

SELECT
	APPT_I,
	FEAT_I,
	EFFT_D,
	SRCE_SYST_C,
	SRCE_SYST_APPT_FEAT_I,
	SRCE_SYST_STND_VALU_Q,
	SRCE_SYST_STND_VALU_R,
	SRCE_SYST_STND_VALU_A,
	ACTL_VALU_Q,
	ACTL_VALU_R,
	ACTL_VALU_A,
	CNCY_C,
	OVRD_FEAT_I,
	OVRD_REAS_C,
	EXPY_D,
	FEAT_SEQN_N,
	FEAT_STRT_D,
	FEE_CHRG_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM,
	CASS_WITHHOLD_RISKBANK_FLAG 
FROM {{ ref('XfmBusinessRules__OutTmpApptFeatDS') }}