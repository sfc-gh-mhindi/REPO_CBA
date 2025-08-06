{{ config(materialized='view', tags=['LdMAP_CSE_SM_CASE']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_map__cse__sm__case__i__cse__com__bus__app__prod__ccl__pl__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_map__cse__sm__case__i__cse__com__bus__app__prod__ccl__pl__app__prod__20060101")  }})
TgtMapCseSmCaseInsertDS AS (
	SELECT SM_CASE_ID,
		TARG_I,
		TARG_SUBJ,
		EFFT_D
	FROM _cba__app_csel4_csel4dev_dataset_map__cse__sm__case__i__cse__com__bus__app__prod__ccl__pl__app__prod__20060101
)

SELECT * FROM TgtMapCseSmCaseInsertDS