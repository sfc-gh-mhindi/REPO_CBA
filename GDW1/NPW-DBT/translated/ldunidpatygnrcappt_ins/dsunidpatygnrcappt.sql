{{ config(materialized='view', tags=['LdUnidPatyGnrcAppt_Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__unid__paty__gnrc AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__unid__paty__gnrc")  }})
dsUnidPatyGnrcAppt AS (
	SELECT UNID_PATY_I,
		APPT_I,
		EFFT_D,
		EXPY_D,
		REL_TYPE_C,
		REL_REAS_C,
		REL_STUS_C,
		REL_LEVL_C,
		SRCE_SYST_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__comm__unid__paty__gnrc
)

SELECT * FROM dsUnidPatyGnrcAppt