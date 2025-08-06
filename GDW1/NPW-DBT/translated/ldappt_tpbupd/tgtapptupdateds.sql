{{ config(materialized='view', tags=['LdAppt_TPBUpd']) }}

WITH 
_cba__app_hlt_dev_dataset_tpb__appt__u__cse__com__bus__ccl__chl__com__app__20080624 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_tpb__appt__u__cse__com__bus__ccl__chl__com__app__20080624")  }})
TgtApptUpdateDS AS (
	SELECT APPT_I,
		APPT_C,
		APPT_FORM_C,
		STUS_TRAK_I,
		APPT_ORIG_C,
		ORIG_APPT_SRCE_C,
		APPT_RECV_S,
		REL_MGR_STAT_C,
		APPT_RECV_D,
		APPT_RECV_T
	FROM _cba__app_hlt_dev_dataset_tpb__appt__u__cse__com__bus__ccl__chl__com__app__20080624
)

SELECT * FROM TgtApptUpdateDS