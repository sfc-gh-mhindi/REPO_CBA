{{ config(materialized='incremental', alias='tmp_appt_stus_reas', incremental_strategy='append', tags=['LdTMP_APPT_STUS_REASFrmXfm']) }}

SELECT
	APPT_I
	STUS_C
	STUS_REAS_TYPE_C
	STRT_S
	END_S
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptStusReasDS') }}