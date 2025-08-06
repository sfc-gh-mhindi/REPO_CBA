{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_cse__xs__chl__cpl__bus__app__ans__tmp__hl__app__ans', incremental_strategy='insert_overwrite', tags=['Ext_APP_ANS_XFERFrmExt']) }}

SELECT
	EVNT_I,
	QSTN_C,
	EFFT_D,
	RESP_C,
	RESP_CMMT_X,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	SRCE_SYST_EVNT_I,
	SRCE_SYST_C,
	EVNT_ACTV_TYPE_C_XS,
	EVNT_ACTV_TYPE_C,
	DEPT_ROLE_C,
	DEPT_I,
	EMPL_I,
	EVNT_PATY_ROLE_TYPE_C_EE,
	EVNT_PATY_ROLE_TYPE_C,
	SRCE_SYST_PATY_I,
	PATY_I,
	RELD_EVNT_I,
	EVNT_REL_TYPE_C,
	APPT_QLFY_C,
	MOD_TIMESTAMP,
	RUN_STRM 
FROM {{ ref('Transformer_259') }}