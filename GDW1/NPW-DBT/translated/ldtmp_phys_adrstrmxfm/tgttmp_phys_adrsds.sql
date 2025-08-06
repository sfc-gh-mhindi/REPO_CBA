{{ config(materialized='view', tags=['LdTMP_PHYS_ADRSTrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__phys__adrs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__phys__adrs")  }})
TgtTmp_Phys_AdrsDS AS (
	SELECT ADRS_I,
		PHYS_ADRS_TYPE_C,
		ADRS_LINE_1_X,
		ADRS_LINE_2_X,
		SURB_X,
		CITY_X,
		PCOD_C,
		STAT_C,
		ISO_CNTY_C,
		EFFT_D,
		EXPY_D,
		RUN_STRM,
		CHL_PRCP_SCUY_FLAG
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__phys__adrs
)

SELECT * FROM TgtTmp_Phys_AdrsDS