{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__i__cse__cpl__bus__fee__margin__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_RPAYFrmTMP_APPT_PDCT_RPAY']) }}

SELECT
	APPT_PDCT_I,
	RPAY_TYPE_C,
	EFFT_D,
	PAYT_FREQ_C,
	STRT_RPAY_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctRpayInsertDS') }}