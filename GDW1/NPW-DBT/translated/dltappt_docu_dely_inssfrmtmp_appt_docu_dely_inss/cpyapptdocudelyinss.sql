{{ config(materialized='view', tags=['DltAPPT_DOCU_DELY_INSSFrmTMP_APPT_DOCU_DELY_INSS']) }}

WITH CpyApptDocuDelyInss AS (
	SELECT
		NEW_APPT_I,
		NEW_DOCU_DELY_RECV_C,
		{{ ref('SrcTmpApptDocuDelyInssTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptDocuDelyInssTera') }}.OLD_DOCU_DELY_RECV_C AS NEW_DOCU_DELY_RECV_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptDocuDelyInssTera') }}
)

SELECT * FROM CpyApptDocuDelyInss