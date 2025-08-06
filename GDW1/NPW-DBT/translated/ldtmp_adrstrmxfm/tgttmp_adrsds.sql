{{ config(materialized='view', tags=['LdTMP_ADRSTrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__prty__adrs__adrs AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__prty__adrs__adrs")  }})
TgtTmp_AdrsDS AS (
	SELECT ADRS_I,
		ADRS_TYPE_C,
		SRCE_SYST_C,
		ADRS_QLFY_C,
		SRCE_SYST_ADRS_I,
		SRCE_SYST_ADRS_SEQN_N,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__prty__adrs__adrs
)

SELECT * FROM TgtTmp_AdrsDS