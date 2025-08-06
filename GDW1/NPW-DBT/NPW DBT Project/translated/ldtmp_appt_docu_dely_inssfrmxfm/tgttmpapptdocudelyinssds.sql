{{ config(materialized='view', tags=['LdTMP_APPT_DOCU_DELY_INSSFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__docu__dely__inss AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__docu__dely__inss")  }})
TgtTmpApptDocuDelyInssDS AS (
	SELECT APPT_I,
		DOCU_DELY_RECV_C,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__docu__dely__inss
)

SELECT * FROM TgtTmpApptDocuDelyInssDS