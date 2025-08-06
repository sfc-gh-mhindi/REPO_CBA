{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_evnt__empl__u__cse__xs__chl__cpl__bus__app__ans__20080214', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT_EMPL']) }}

SELECT
	EVNT_I,
	EVNT_PATY_ROLE_TYPE_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS') }}