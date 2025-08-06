{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__aset__i__cse__chl__bus__hlm__app__sec__20100616', incremental_strategy='insert_overwrite', tags=['DltAPPT_ASETFrmTMP_APPT_ASET1']) }}

SELECT
	APPT_I,
	ASET_I,
	PRIM_SECU_F,
	efft_d,
	expy_d,
	pros_key_efft_i,
	pros_key_expy_i,
	eror_seqn_i,
	ASET_SETL_REQD 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatInsertDS') }}