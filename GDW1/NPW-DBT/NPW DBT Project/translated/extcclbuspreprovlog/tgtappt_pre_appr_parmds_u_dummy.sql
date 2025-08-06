{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__pre__appr__parm__u__cse__ccl__bus__preapproval__log__20090703', incremental_strategy='insert_overwrite', tags=['ExtCclBusPreprovLog']) }}

SELECT
	APPT_I,
	APPT_PRE_APPR_PARM_I,
	SRCE_SYST_PRE_APPR_PARM_I,
	OVDR_INDX_R,
	BUFR_R,
	UNSC_CAPL_MIN_TSHD_A,
	UNSC_CAPL_MAX_TSHD_A,
	WRST_RISK_CRGD_C,
	CRGD_MULT_R,
	FORM_MULT_R,
	APPT_MULT_R,
	CRGD_MIN_APLC_A,
	CRGD_HIGH_CAPL_TSHD_A,
	PRE_APPR_A,
	EFFT_D,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I 
FROM {{ ref('Trm__Dummy_U_DS') }}