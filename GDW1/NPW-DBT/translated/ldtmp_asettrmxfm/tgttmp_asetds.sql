{{ config(materialized='view', tags=['LdTMP_ASETTrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset")  }})
TgtTmp_AsetDS AS (
	SELECT ASET_I,
		SECU_CODE_C,
		SECU_CATG_C,
		SRCE_SYST_ASET_I,
		SRCE_SYST_C,
		ASET_C,
		ORIG_SRCE_SYST_ASET_I,
		ORIG_SRCE_SYST_C,
		ENVT_F,
		ASET_X,
		ASET_LIBL_C,
		AL_CATG_C,
		DUPL_ASET_F,
		EFFT_D,
		EXPY_D,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset
)

SELECT * FROM TgtTmp_AsetDS