{{ config(materialized='view', tags=['DltAPPT_STUS_REASFrmTMP_APPT_STUS_REAS']) }}

WITH CpyApptStusReas AS (
	SELECT
		NEW_APPT_I,
		NEW_STUS_C,
		NEW_STUS_REAS_TYPE_C,
		NEW_STRT_S,
		NEW_END_S,
		{{ ref('SrcTmpApptStusReasTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptStusReasTera') }}.OLD_STUS_C AS NEW_STUS_C,
		{{ ref('SrcTmpApptStusReasTera') }}.OLD_STUS_REAS_TYPE_C AS NEW_STUS_REAS_TYPE_C,
		{{ ref('SrcTmpApptStusReasTera') }}.OLD_STRT_S AS NEW_STRT_S,
		{{ ref('SrcTmpApptStusReasTera') }}.OLD_END_S AS NEW_END_S,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptStusReasTera') }}
)

SELECT * FROM CpyApptStusReas