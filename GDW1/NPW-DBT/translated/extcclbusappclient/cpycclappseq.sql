{{ config(materialized='view', tags=['ExtCclBusAppClient']) }}

WITH CpyCclAppSeq AS (
	SELECT
		{{ ref('SrcCclAppSeq') }}.CCL_APP_ID AS APPT_I,
		{{ ref('SrcCclAppSeq') }}.CIF_CODE AS PATY_I,
		APPLICANT_FLAG
	FROM {{ ref('SrcCclAppSeq') }}
)

SELECT * FROM CpyCclAppSeq