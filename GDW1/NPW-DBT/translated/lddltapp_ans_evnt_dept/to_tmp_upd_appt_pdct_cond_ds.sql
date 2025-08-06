{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_dataset_evnt__dept__u__cse__xs__chl__cpl__bus__app__ans__20080124', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_EVNT_DEPT']) }}

SELECT
	EVNT_I,
	DEPT_ROLE_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS') }}