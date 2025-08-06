{{ config(materialized='view', tags=['ExtHL_APP_Adhoc']) }}

WITH XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((XfmCheckHlAppIdNulls.HL_APP_ID)) THEN (XfmCheckHlAppIdNulls.HL_APP_ID) ELSE 0)) = 0) Then 'REJ2010' else  if Trim(XfmCheckHlAppIdNulls.HL_APP_ID) = '' Then 'REJ2010' else  if num(XfmCheckHlAppIdNulls.HL_APP_ID) then ( if (StringToDecimal(TRIM(XfmCheckHlAppIdNulls.HL_APP_ID)) = 0) Then 'REJ2010' else '') else '',
		IFF(TRIM(IFF({{ ref('CpyHlApponeoffSeq') }}.HL_APP_ID IS NOT NULL, {{ ref('CpyHlApponeoffSeq') }}.HL_APP_ID, 0)) = 0, 'REJ2010', IFF(TRIM({{ ref('CpyHlApponeoffSeq') }}.HL_APP_ID) = '', 'REJ2010', IFF(NUM({{ ref('CpyHlApponeoffSeq') }}.HL_APP_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('CpyHlApponeoffSeq') }}.HL_APP_ID)) = 0, 'REJ2010', ''), ''))) AS ErrorCode,
		HL_APP_ID,
		HL_APP_PROD_ID,
		PEXA_FLAG
	FROM {{ ref('CpyHlApponeoffSeq') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted