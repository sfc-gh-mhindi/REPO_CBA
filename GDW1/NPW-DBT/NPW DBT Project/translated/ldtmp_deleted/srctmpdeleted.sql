{{ config(materialized='view', tags=['LdTMP_DELETED']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__deleted__20061015 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__deleted__20061015")  }})
srcTmpDeleted AS (
	SELECT TGT_TBL_NAME,
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
	FROM _cba__app_csel4_csel4dev_dataset_tmp__deleted__20061015
)

SELECT * FROM srcTmpDeleted