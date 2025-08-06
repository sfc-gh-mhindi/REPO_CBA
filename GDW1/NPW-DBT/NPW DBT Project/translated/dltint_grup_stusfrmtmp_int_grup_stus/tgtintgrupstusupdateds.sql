{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__u__cse__cpl__bus__fee__margin__20060101', incremental_strategy='insert_overwrite', tags=['DltINT_GRUP_STUSFrmTMP_INT_GRUP_STUS']) }}

SELECT
	INT_GRUP_I,
	STRT_S,
	STUS_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtIntGrupStusUpdateDS') }}