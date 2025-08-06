{{ config(materialized='view', tags=['LdTMP_CSE_APPT_CPGNFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__cpgn AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__cpgn")  }})
SrcCseApptCpgnDS AS (
	SELECT APPT_I,
		CSE_CPGN_CODE_X,
		EFFT_D,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__cpgn
)

SELECT * FROM SrcCseApptCpgnDS