{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__amt__3__i__cse__ccc__bus__app__prod__20120427', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_3']) }}

SELECT
	APPT_PDCT_I,
	AMT_TYPE_C,
	EFFT_D,
	EXPY_D,
	CNCY_C,
	APPT_PDCT_A,
	SRCE_SYST_C,
	XCES_AMT_REAS_X,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	CNCY_CONV_R,
	DISC_CNCY_CONV_R,
	DISC_CNCY_DEAL_AUTN_N,
	SRCE_SYST_APPT_PDCT_AMT_I,
	APPT_PDCT_AUD_EQAL_A,
	PAYT_METH_TYPE_C 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctAmtInsertDS') }}