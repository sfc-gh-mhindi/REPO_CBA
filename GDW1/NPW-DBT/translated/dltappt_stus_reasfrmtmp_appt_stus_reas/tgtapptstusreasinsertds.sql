{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__i__cse__cpl__bus__fee__margin__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_STUS_REASFrmTMP_APPT_STUS_REAS']) }}

SELECT
	APPT_I,
	STUS_C,
	STUS_REAS_TYPE_C,
	STRT_S,
	END_S,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptStusReasInsertDS') }}