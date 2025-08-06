{{ config(materialized='view', tags=['LdApptPdctCpgnIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__cpgn__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__cpgn__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptPdctCpgnInsertDS AS (
	SELECT APPT_PDCT_I,
		CPGN_TYPE_C,
		CPGN_I,
		REL_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_appt__pdct__cpgn__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptPdctCpgnInsertDS