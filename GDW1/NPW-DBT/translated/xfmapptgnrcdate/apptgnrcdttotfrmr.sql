{{ config(materialized='view', tags=['XfmApptGnrcDate']) }}

WITH 
_cba__app_csel2_dev_dataset_appt__gnrc__date__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel2_dev_dataset_appt__gnrc__date__20100614")  }})
ApptGnrcDtToTfrmr AS (
	SELECT EVNT_I,
		VALU_D,
		VALU_T,
		APPT_I
	FROM _cba__app_csel2_dev_dataset_appt__gnrc__date__20100614
)

SELECT * FROM ApptGnrcDtToTfrmr