{{ config(materialized='view', tags=['LdApptUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__u__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__u__cse__cpl__bus__fee__margin__20060101")  }})
TgtApptUpdateDS AS (
	SELECT APPT_I,
		APPT_C,
		APPT_FORM_C,
		STUS_TRAK_I,
		APPT_ORIG_C
	FROM _cba__app_csel4_csel4dev_dataset_appt__u__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM TgtApptUpdateDS