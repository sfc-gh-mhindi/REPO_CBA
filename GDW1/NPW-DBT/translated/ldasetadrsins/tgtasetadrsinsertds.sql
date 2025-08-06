{{ config(materialized='view', tags=['LdAsetAdrsIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_aset__adrs__i__cse__com__bus__app__ccl__app__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_aset__adrs__i__cse__com__bus__app__ccl__app__20060101")  }})
TgtAsetAdrsInsertDS AS (
	SELECT ASET_I,
		ADRS_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_aset__adrs__i__cse__com__bus__app__ccl__app__20060101
)

SELECT * FROM TgtAsetAdrsInsertDS