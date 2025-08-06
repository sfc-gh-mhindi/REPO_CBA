{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_appt__pdct__feat__i__cse__com__bus__app__prod__ccl__pl__app__prod__20071220', incremental_strategy='insert_overwrite', tags=['LdDltClp_App_Pdct_Feat']) }}

SELECT
	APPT_PDCT_I,
	FEAT_I,
	SRCE_SYST_APPT_FEAT_I,
	EFFT_D,
	SRCE_SYST_C,
	SRCE_SYST_APPT_OVRD_I,
	OVRD_FEAT_I,
	SRCE_SYST_STND_VALU_Q,
	SRCE_SYST_STND_VALU_R,
	SRCE_SYST_STND_VALU_A,
	CNCY_C,
	ACTL_VALU_Q,
	ACTL_VALU_R,
	ACTL_VALU_A,
	FEAT_SEQN_N,
	FEAT_STRT_D,
	FEE_CHRG_D,
	OVRD_REAS_C,
	FEE_ADD_TO_TOTL_F,
	FEE_CAPL_F,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XFR_Split__To_Tmp_Insert_USR_TEM_DS') }}