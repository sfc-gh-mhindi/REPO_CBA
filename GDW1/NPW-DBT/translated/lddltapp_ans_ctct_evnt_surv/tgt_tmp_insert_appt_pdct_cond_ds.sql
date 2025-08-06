{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__surv__i__cse__xs__chl__cpl__bus__app__ans__20080124', incremental_strategy='insert_overwrite', tags=['LdDltAPP_ANS_CTCT_EVNT_SURV']) }}

SELECT
	EVNT_I,
	QSTN_C,
	EFFT_D,
	RESP_C,
	RESP_CMMT_X,
	EXPY_D,
	ROW_SECU_ACCS_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XFR_Split__To_Tmp_Insert_USR_TEM_DS') }}