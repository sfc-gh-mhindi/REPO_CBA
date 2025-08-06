{{ config(materialized='view', tags=['LdApptPdctFeatUpdate']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__com__cpo__bus__ncpr__clnt__20101010 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__com__cpo__bus__ncpr__clnt__20101010")  }})
TgtApptInsertDS AS (
	SELECT APPT_PDCT_I,
		FEAT_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__com__cpo__bus__ncpr__clnt__20101010
)

SELECT * FROM TgtApptInsertDS