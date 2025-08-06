{{ config(materialized='view', tags=['DltApptFrmTMP']) }}

WITH XfmInserts AS (
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
		ORIG_APPT_SRCE_C,
		APPT_RECV_S,
		REL_MGR_STAT_C,
		APPT_RECV_D,
		APPT_RECV_T,
		GDW_PROS_ID AS PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		APPT_ENTR_POIT_M
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.dummy = 0
)

SELECT * FROM XfmInserts