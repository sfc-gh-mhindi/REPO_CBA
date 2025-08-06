{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tmp__cse__cpl__bus__fee__margin__appt', incremental_strategy='insert_overwrite', tags=['ExtWRK_APPT_COM_CCL_CHL_APPFrmStg']) }}

SELECT
	APPT_I,
	APPT_C,
	APPT_FORM_C,
	APPT_QLFY_C,
	STUS_TRAK_I,
	APPT_ORIG_C,
	APPT_N,
	SRCE_SYST_C,
	SRCE_SYST_APPT_I,
	APPT_CRAT_D,
	RATE_SEEK_F,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	RUN_STRM,
	ORIG_APPT_SRCE_C,
	APPT_RECV_S,
	REL_MGR_STAT_C,
	APPT_RECV_D,
	APPT_RECV_T 
FROM {{ ref('SrcWrkApptTera') }}