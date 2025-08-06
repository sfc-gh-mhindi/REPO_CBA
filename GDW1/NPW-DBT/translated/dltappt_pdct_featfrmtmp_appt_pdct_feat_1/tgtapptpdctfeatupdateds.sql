{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__feat__1__u__cse__ccl__bus__app__prod__20170306', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_FEATFrmTMP_APPT_PDCT_FEAT_1']) }}

SELECT
	APPT_PDCT_I,
	FEAT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctFeatUpdateDS') }}