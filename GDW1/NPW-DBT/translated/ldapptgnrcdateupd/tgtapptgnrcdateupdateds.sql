{{ config(materialized='view', tags=['LdApptGnrcDateUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__gnrc__date__u__cse__cchl__bus__app__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__gnrc__date__u__cse__cchl__bus__app__20060101")  }})
TgtApptGnrcDateUpdateDS AS (
	SELECT APPT_I,
		EXPY_D,
		PROS_KEY_EXPY_I,
		EFFT_D
	FROM _cba__app_csel4_csel4dev_dataset_appt__gnrc__date__u__cse__cchl__bus__app__20060101
)

SELECT * FROM TgtApptGnrcDateUpdateDS