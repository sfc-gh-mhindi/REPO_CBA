{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_fa__client__undertaking__paty__int__grup', incremental_strategy='insert_overwrite', tags=['XfmDelFlagPATY_INT_GRUP']) }}

SELECT
	INT_GRUP_I,
	SRCE_SYST_PATY_INT_GRUP_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform') }}