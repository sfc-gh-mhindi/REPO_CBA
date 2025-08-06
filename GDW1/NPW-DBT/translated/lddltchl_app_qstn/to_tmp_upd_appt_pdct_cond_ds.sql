{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_appt__qstn__u__cse__clp__bus__appt__qstn__20080402', incremental_strategy='insert_overwrite', tags=['LdDltChl_App_Qstn']) }}

SELECT
	APPT_I,
	PATY_I,
	QSTN_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XFR_Split__To_Tmp_Update_Paty_Int_Grup_DS') }}