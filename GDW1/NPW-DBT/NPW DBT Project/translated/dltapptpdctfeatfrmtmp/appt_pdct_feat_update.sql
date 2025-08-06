{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__pdct__feat__u__cse__com__cpo__bus__ncpr__clnt__20110124', incremental_strategy='insert_overwrite', tags=['DltApptPdctFeatFrmTMP']) }}

SELECT
	APPT_PDCT_I,
	FEAT_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('GetGDWFields') }}