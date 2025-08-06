{{ config(materialized='view', tags=['LdApptPdctCpgnUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__cpgn__u__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__cpgn__u__cse__ccc__bus__app__prod__20060101")  }})
TgtApptPdctCpgnUpdateDS AS (
	SELECT APPT_PDCT_I,
		CPGN_TYPE_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__cpgn__u__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptPdctCpgnUpdateDS