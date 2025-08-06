{{ config(materialized='view', tags=['LdApptPdctDeptInsert']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101")  }})
ApptPdctDeptInsert AS (
	SELECT APPT_PDCT_I,
		DEPT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		BRCH_N,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM ApptPdctDeptInsert