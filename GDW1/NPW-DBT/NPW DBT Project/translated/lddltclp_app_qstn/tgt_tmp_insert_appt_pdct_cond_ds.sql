{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_appt__qstn__i__cse__clp__bus__appt__qstn__20080402', incremental_strategy='insert_overwrite', tags=['LdDltClp_App_Qstn']) }}

SELECT
	APPT_I,
	QSTN_C,
	EFFT_D,
	RESP_C,
	RESP_CMMT_X,
	EXPY_D,
	PATY_I,
	ROW_SECU_ACCS_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XFR_Split__To_Tmp_Insert_USR_TEM_DS') }}