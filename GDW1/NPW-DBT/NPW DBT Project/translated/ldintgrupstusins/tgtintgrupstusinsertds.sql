{{ config(materialized='view', tags=['LdIntGrupStusIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtIntGrupStusInsertDS AS (
	SELECT INT_GRUP_I,
		STRT_S,
		STUS_C,
		STRT_D,
		STRT_T,
		EMPL_I,
		END_S,
		END_D,
		END_T,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtIntGrupStusInsertDS