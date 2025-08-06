{{ config(materialized='view', tags=['LdApptGnrcDatechUpd']) }}

WITH 
_cba__app01_csel4_dev_dataset_appt__gnrc__date__u__cse__ccl__cli__date__exp__aud__20100412 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_appt__gnrc__date__u__cse__ccl__cli__date__exp__aud__20100412")  }})
APPT_GNRC_DATE_U AS (
	SELECT APPT_I,
		DATE_ROLE_C,
		EXPY_D,
		PROS_KEY_EXPY_I,
		EFFT_D
	FROM _cba__app01_csel4_dev_dataset_appt__gnrc__date__u__cse__ccl__cli__date__exp__aud__20100412
)

SELECT * FROM APPT_GNRC_DATE_U