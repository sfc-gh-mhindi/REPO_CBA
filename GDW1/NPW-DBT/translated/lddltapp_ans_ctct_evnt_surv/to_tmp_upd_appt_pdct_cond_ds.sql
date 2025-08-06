{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__surv__u__cse__xs__chl__cpl__bus__app__ans__20080124', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_CTCT_EVNT_SURV']) }}

SELECT
	EVNT_I,
	QSTN_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS') }}