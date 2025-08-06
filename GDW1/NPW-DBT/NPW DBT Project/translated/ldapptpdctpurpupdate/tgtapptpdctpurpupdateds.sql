{{ config(materialized='view', tags=['LdApptPdctPurpUpdate']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__purp__u__cse__com__cpo__ncpr__clnt__empl__20101030 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__purp__u__cse__com__cpo__ncpr__clnt__empl__20101030")  }})
TgtApptPdctPurpUpdateDS AS (
	SELECT APPT_PDCT_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__purp__u__cse__com__cpo__ncpr__clnt__empl__20101030
)

SELECT * FROM TgtApptPdctPurpUpdateDS