{{ config(materialized='view', tags=['DltAPPTTBPFrmTMP_APPT']) }}

WITH CpyAppt AS (
	SELECT
		NEW_APPT_I,
		NEW_APPT_C,
		NEW_APPT_FORM_C,
		NEW_STUS_TRAK_I,
		NEW_APPT_ORIG_C,
		NEW_APPT_ORIG_SYST_C,
		NEW_APPT_RECV_S,
		NEW_REL_MGR_STAT_C,
		NEW_APPT_RECV_D,
		NEW_APPT_RECV_T,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_C AS NEW_APPT_C,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_FORM_C AS NEW_APPT_FORM_C,
		{{ ref('SrcTmpApptTera') }}.OLD_STUS_TRAK_I AS NEW_STUS_TRAK_I,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_ORIG_C AS NEW_APPT_ORIG_C,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_ORIG_SYST_C AS NEW_APPT_ORIG_SYST_C,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_RECV_S AS NEW_APPT_RECV_S,
		{{ ref('SrcTmpApptTera') }}.OLD_REL_MGR_STAT_C AS NEW_REL_MGR_STAT_C,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_RECV_D AS NEW_APPT_RECV_D,
		{{ ref('SrcTmpApptTera') }}.OLD_APPT_RECV_T AS NEW_APPT_RECV_T,
		NEW_APPT_QLFY_C,
		NEW_APPT_N,
		NEW_SRCE_SYST_C,
		NEW_SRCE_SYST_APPT_I,
		NEW_APPT_CRAT_D,
		NEW_RATE_SEEK_F
	FROM {{ ref('SrcTmpApptTera') }}
)

SELECT * FROM CpyAppt