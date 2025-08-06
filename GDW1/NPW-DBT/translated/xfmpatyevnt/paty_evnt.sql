{{ config(materialized='incremental', alias='_cba__app01_csel4_dev_dataset_paty__evnt__i__cse__onln__bus__clnt__rm__rate__20100303', incremental_strategy='insert_overwrite', tags=['XfmPatyEvnt']) }}

SELECT
	EVNT_I,
	SRCE_SYST_PATY_I,
	EVNT_PATY_ROLE_TYPE_C,
	SRCE_SYST_C,
	EFFT_D,
	PATY_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmInserts') }}