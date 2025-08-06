{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_appt__pdct__feat__u__cse__com__bus__app__prod__ccl__pl__app__prod__20071220', incremental_strategy='insert_overwrite', tags=['LdDltClp_App_Pdct_Feat']) }}

SELECT
	APPT_PDCT_I,
	FEAT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS') }}