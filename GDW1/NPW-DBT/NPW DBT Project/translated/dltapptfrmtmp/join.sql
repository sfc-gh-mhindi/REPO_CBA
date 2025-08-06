{{ config(materialized='view', tags=['DltApptFrmTMP']) }}

WITH Join AS (
	SELECT
		{{ ref('XfmAppt') }}.APPT_I,
		{{ ref('XfmAppt') }}.APPT_C,
		{{ ref('XfmAppt') }}.APPT_FORM_C,
		{{ ref('XfmAppt') }}.APPT_QLFY_C,
		{{ ref('XfmAppt') }}.STUS_TRAK_I,
		{{ ref('XfmAppt') }}.APPT_ORIG_C,
		{{ ref('XfmAppt') }}.APPT_N,
		{{ ref('XfmAppt') }}.SRCE_SYST_C,
		{{ ref('XfmAppt') }}.SRCE_SYST_APPT_I,
		XfmXfmAppt.APPT_CRAT_D,
		{{ ref('XfmAppt') }}.RATE_SEEK_F,
		{{ ref('XfmAppt') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmAppt') }}.EROR_SEQN_I,
		{{ ref('XfmAppt') }}.RUN_STRM,
		{{ ref('XfmAppt') }}.ORIG_APPT_SRCE_C,
		{{ ref('XfmAppt') }}.APPT_RECV_S,
		{{ ref('XfmAppt') }}.REL_MGR_STAT_C,
		{{ ref('XfmAppt') }}.APPT_RECV_D,
		{{ ref('XfmAppt') }}.APPT_RECV_T,
		{{ ref('XfmAppt') }}.APPT_ENTR_POIT_M,
		{{ ref('GdwAppt') }}.dummy
	FROM {{ ref('XfmAppt') }}
	LEFT JOIN {{ ref('GdwAppt') }} ON {{ ref('XfmAppt') }}.APPT_I = {{ ref('GdwAppt') }}.APPT_I
)

SELECT * FROM Join