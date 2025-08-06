{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_CPGNFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__ccc__bus__app__prod__appt__pdct__cpgn AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__ccc__bus__app__prod__appt__pdct__cpgn")  }})
SrcApptPdctCpgnDS AS (
	SELECT APPT_PDCT_I,
		CPGN_TYPE_C,
		CPGN_I,
		REL_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__ccc__bus__app__prod__appt__pdct__cpgn
)

SELECT * FROM SrcApptPdctCpgnDS