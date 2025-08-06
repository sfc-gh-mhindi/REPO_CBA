{{ config(materialized='view', tags=['DltAPPT_STUSFrmTMP_APPT_STUS']) }}

WITH CpyApptStus AS (
	SELECT
		NEW_APPT_I,
		NEW_STUS_C,
		NEW_STRT_S,
		NEW_END_D,
		NEW_END_T,
		NEW_END_S,
		{{ ref('SrcTmpApptStusTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptStusTera') }}.OLD_STUS_C AS NEW_STUS_C,
		{{ ref('SrcTmpApptStusTera') }}.OLD_STRT_S AS NEW_STRT_S,
		{{ ref('SrcTmpApptStusTera') }}.OLD_END_D AS NEW_END_D,
		{{ ref('SrcTmpApptStusTera') }}.OLD_END_T AS NEW_END_T,
		{{ ref('SrcTmpApptStusTera') }}.OLD_END_S AS NEW_END_S,
		NEW_STRT_D,
		NEW_STRT_T,
		NEW_EMPL_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptStusTera') }}
)

SELECT * FROM CpyApptStus