{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__feat__i__cse__ccl__bus__app__fee__20060616', incremental_strategy='insert_overwrite', tags=['DltAPPT_ASETFrmTMP_APPT_ASET']) }}

SELECT
	APPT_I,
	ASET_I,
	PRIM_SECU_F,
	efft_d,
	expy_d,
	pros_key_efft_i,
	pros_key_expy_i,
	eror_seqn_i 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatInsertDS') }}