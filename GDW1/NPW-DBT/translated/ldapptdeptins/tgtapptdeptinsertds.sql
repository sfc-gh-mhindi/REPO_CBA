{{ config(materialized='view', tags=['LdApptDeptIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__dept__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__dept__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptDeptInsertDS AS (
	SELECT APPT_I,
		DEPT_ROLE_C,
		EFFT_D,
		DEPT_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__dept__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptDeptInsertDS