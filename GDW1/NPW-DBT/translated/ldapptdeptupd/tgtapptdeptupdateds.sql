{{ config(materialized='view', tags=['LdApptDeptUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__dept__u__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__dept__u__cse__ccc__bus__app__prod__20060101")  }})
TgtApptDeptUpdateDS AS (
	SELECT DEPT_I,
		APPT_I,
		DEPT_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__dept__u__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptDeptUpdateDS