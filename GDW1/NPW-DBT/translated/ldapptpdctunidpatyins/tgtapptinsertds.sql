{{ config(materialized='view', tags=['LdApptPdctUnidPatyIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__unid__paty__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__unid__paty__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptInsertDS AS (
	SELECT APPT_PDCT_I,
		PATY_ROLE_C,
		SRCE_SYST_PATY_I,
		EFFT_D,
		SRCE_SYST_C,
		UNID_PATY_CATG_C,
		PATY_M,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__unid__paty__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptInsertDS