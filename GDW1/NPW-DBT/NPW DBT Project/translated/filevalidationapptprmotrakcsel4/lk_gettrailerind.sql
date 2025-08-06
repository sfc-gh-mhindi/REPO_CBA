{{ config(materialized='view', tags=['FileValidationApptPrmoTrakCSEL4']) }}

WITH lk_GetTrailerInd AS (
	SELECT
		{{ ref('sq_SrceFileData') }}.RECD_TYPE,
		{{ ref('sq_SrceFileData') }}.REST_OF_RECD,
		{{ ref('sq_SrceFileData') }}.ROW_NUMBER,
		{{ ref('xf_LastRecdNumb') }}.ROW_NUMBER AS ROW_NUMBER_1
	FROM {{ ref('sq_SrceFileData') }}
	LEFT JOIN {{ ref('xf_LastRecdNumb') }} ON {{ ref('sq_SrceFileData') }}.ROW_NUMBER = {{ ref('xf_LastRecdNumb') }}.ROW_NUMBER
)

SELECT * FROM lk_GetTrailerInd