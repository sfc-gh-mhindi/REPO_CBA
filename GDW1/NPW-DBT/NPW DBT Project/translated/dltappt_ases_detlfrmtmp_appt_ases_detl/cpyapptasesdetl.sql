{{ config(materialized='view', tags=['DltAPPT_ASES_DETLFrmTMP_APPT_ASES_DETL']) }}

WITH CpyApptAsesDetl AS (
	SELECT
		NEW_APPT_I,
		NEW_AMT_TYPE_C,
		NEW_APPT_ASES_A,
		{{ ref('SrcTmpApptAsesDetlTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptAsesDetlTera') }}.OLD_AMT_TYPE_C AS NEW_AMT_TYPE_C,
		{{ ref('SrcTmpApptAsesDetlTera') }}.OLD_APPT_ASES_A AS NEW_APPT_ASES_A,
		NEW_CNCY_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptAsesDetlTera') }}
)

SELECT * FROM CpyApptAsesDetl