{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__i__cse__cpl__bus__fee__margin__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_UNID_PATYFrmTMP_APPT_PDCT_UNID_PATY']) }}

SELECT
	APPT_PDCT_I,
	PATY_ROLE_C,
	SRCE_SYST_PATY_I,
	EFFT_D,
	SRCE_SYST_C,
	UNID_PATY_CATG_C,
	PATY_M,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctUnidPatyInsertDS') }}