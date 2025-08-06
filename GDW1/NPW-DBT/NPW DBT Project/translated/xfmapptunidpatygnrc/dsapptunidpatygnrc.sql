{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__appt__unid__paty__gnrc', incremental_strategy='insert_overwrite', tags=['XfmApptUnidPatyGnrc']) }}

SELECT
	UNID_PATY_I,
	EFFT_D,
	PATY_TYPE_C,
	PATY_ROLE_C,
	PROS_KEY_EFFT_I,
	SRCE_SYST_C,
	PATY_QLFY_C,
	SRCE_SYST_PATY_I 
FROM {{ ref('Lk_BusRules') }}