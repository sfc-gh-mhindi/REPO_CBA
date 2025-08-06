{{ config(materialized='view', tags=['LdCseApptHlCondApprUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__acct__u__cse__chk__bus__app__20150117 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__acct__u__cse__chk__bus__app__20150117")  }})
TgtApptHlCondApprUpdateDS AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__acct__u__cse__chk__bus__app__20150117
)

SELECT * FROM TgtApptHlCondApprUpdateDS