{{ config(materialized='view', tags=['LdApptAsetIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__i__cse__com__bus__app__ccl__app__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__i__cse__com__bus__app__ccl__app__20060101")  }})
TgtApptAsetInsertDS AS (
	SELECT APPT_I,
		ASET_I,
		PRIM_SECU_F,
		efft_d,
		expy_d,
		pros_key_efft_i,
		pros_key_expy_i,
		eror_seqn_i
	FROM _cba__app_csel4_csel4dev_dataset_appt__i__cse__com__bus__app__ccl__app__20060101
)

SELECT * FROM TgtApptAsetInsertDS