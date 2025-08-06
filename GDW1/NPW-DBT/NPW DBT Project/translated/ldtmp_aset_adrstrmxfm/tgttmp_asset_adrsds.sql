{{ config(materialized='view', tags=['LdTMP_ASET_ADRSTrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset__adrs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset__adrs")  }})
TgtTmp_Asset_AdrsDS AS (
	SELECT ASET_I,
		ADRS_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		EROR_SEQN_I,
		RUN_STRM,
		CHL_PRCP_SCUY_FLAG
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__aset__adrs
)

SELECT * FROM TgtTmp_Asset_AdrsDS