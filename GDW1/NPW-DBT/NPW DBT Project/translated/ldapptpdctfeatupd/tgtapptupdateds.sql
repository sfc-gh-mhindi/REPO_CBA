{{ config(materialized='view', tags=['LdApptPdctFeatUpd']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_appt__pdct__feat__u__cse__clp__bus__appt__pdct__feat__20071220 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_appt__pdct__feat__u__cse__clp__bus__appt__pdct__feat__20071220")  }})
TgtApptUpdateDS AS (
	SELECT APPT_PDCT_I,
		FEAT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_lpxs_lpxs__dev_dataset_appt__pdct__feat__u__cse__clp__bus__appt__pdct__feat__20071220
)

SELECT * FROM TgtApptUpdateDS