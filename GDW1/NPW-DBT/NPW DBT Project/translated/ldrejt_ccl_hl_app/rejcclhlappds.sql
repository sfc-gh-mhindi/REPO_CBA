{{ config(materialized='view', tags=['LdREJT_CCL_HL_APP']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__cclhlapp__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__cclhlapp__nulls__rejects")  }})
RejCclHlAppDS AS (
	SELECT CCL_HL_APP_ID,
		CCL_APP_ID,
		HL_APP_ID,
		LMI_AMT,
		HL_PACKAGE_CAT_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__ccl__bus__hl__app__cclhlapp__nulls__rejects
)

SELECT * FROM RejCclHlAppDS