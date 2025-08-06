{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__deleted__20061015', incremental_strategy='insert_overwrite', tags=['MergeTMP_DELETED']) }}

SELECT
	TGT_TBL_NAME,
	SRC_TBL_NAME,
	DLTD_TABL_NAME,
	DLTD_KEY1,
	DLTD_KEY1_VALU,
	DLTD_KEY2,
	DLTD_KEY2_VALU,
	DLTD_KEY3,
	DLTD_KEY3_VALU,
	DLTD_KEY4,
	DLTD_KEY4_VALU,
	DLTD_KEY5,
	DLTD_KEY5_VALU 
FROM {{ ref('fnlDeleted') }}