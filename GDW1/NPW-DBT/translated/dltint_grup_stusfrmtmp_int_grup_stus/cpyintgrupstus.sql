{{ config(materialized='view', tags=['DltINT_GRUP_STUSFrmTMP_INT_GRUP_STUS']) }}

WITH CpyIntGrupStus AS (
	SELECT
		NEW_INT_GRUP_I,
		NEW_STRT_S,
		NEW_STUS_C,
		NEW_END_S,
		NEW_END_D,
		NEW_END_T,
		{{ ref('SrcTmpIntGrupStusTera') }}.OLD_INT_GRUP_I AS NEW_INT_GRUP_I,
		{{ ref('SrcTmpIntGrupStusTera') }}.OLD_STRT_S AS NEW_STRT_S,
		{{ ref('SrcTmpIntGrupStusTera') }}.OLD_STUS_C AS NEW_STUS_C,
		{{ ref('SrcTmpIntGrupStusTera') }}.OLD_END_S AS NEW_END_S,
		{{ ref('SrcTmpIntGrupStusTera') }}.OLD_END_D AS NEW_END_D,
		{{ ref('SrcTmpIntGrupStusTera') }}.OLD_END_T AS NEW_END_T,
		NEW_STRT_D,
		NEW_STRT_T,
		NEW_EMPL_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpIntGrupStusTera') }}
)

SELECT * FROM CpyIntGrupStus