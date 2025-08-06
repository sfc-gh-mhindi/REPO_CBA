{{ config(materialized='view', tags=['LdTMP_APPT_GNRC_DATE_FrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__gnrc__date AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__gnrc__date")  }})
SrcApptGnrcDateDS AS (
	SELECT APPT_I,
		DATE_ROLE_C,
		EFFT_D,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		MODF_S,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__appt__gnrc__date
)

SELECT * FROM SrcApptGnrcDateDS