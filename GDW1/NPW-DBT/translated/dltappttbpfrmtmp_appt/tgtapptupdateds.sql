{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tpb__appt__u__cse__com__bus__ccl__chl__com__app__20080624', incremental_strategy='insert_overwrite', tags=['DltAPPTTBPFrmTMP_APPT']) }}

SELECT
	APPT_I,
	APPT_C,
	APPT_FORM_C,
	STUS_TRAK_I,
	APPT_ORIG_C,
	ORIG_APPT_SRCE_C,
	APPT_RECV_S,
	REL_MGR_STAT_C,
	APPT_RECV_D,
	APPT_RECV_T 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptUpdateDS') }}