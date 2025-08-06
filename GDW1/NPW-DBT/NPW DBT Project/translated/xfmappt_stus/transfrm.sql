{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH Transfrm AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans.SM_CASE_ID) Then 'Y' Else  if Trim(ToTrans.SM_CASE_ID) = 0 Then 'Y' Else  if Trim(( IF IsNotNull((ToTrans.SM_CASE_ID)) THEN (ToTrans.SM_CASE_ID) ELSE "")) = '' Then 'Y' Else 'N',
		IFF({{ ref('CseComBusApp') }}.SM_CASE_ID IS NULL, 'Y', IFF(TRIM({{ ref('CseComBusApp') }}.SM_CASE_ID) = 0, 'Y', IFF(TRIM(IFF({{ ref('CseComBusApp') }}.SM_CASE_ID IS NOT NULL, {{ ref('CseComBusApp') }}.SM_CASE_ID, '')) = '', 'Y', 'N'))) AS svIsNullSmCaseId,
		-- *SRC*: \(20)if ToTrans.SUBTYPE_CODE <> 'PO' Then 'Y' Else 'N',
		IFF({{ ref('CseComBusApp') }}.SUBTYPE_CODE <> 'PO', 'Y', 'N') AS svIsNullSubTypeCode,
		-- *SRC*: \(20)If IsNull(ToTrans.APP_ID) Then 'Y' Else  if Trim(ToTrans.APP_ID) = 0 Then 'Y' Else  if Trim(( IF IsNotNull((ToTrans.APP_ID)) THEN (ToTrans.APP_ID) ELSE "")) = '' Then 'Y' Else 'N',
		IFF({{ ref('CseComBusApp') }}.APP_ID IS NULL, 'Y', IFF(TRIM({{ ref('CseComBusApp') }}.APP_ID) = 0, 'Y', IFF(TRIM(IFF({{ ref('CseComBusApp') }}.APP_ID IS NOT NULL, {{ ref('CseComBusApp') }}.APP_ID, '')) = '', 'Y', 'N'))) AS svIsNullAppId,
		APP_ID,
		SUBTYPE_CODE,
		SM_CASE_ID
	FROM {{ ref('CseComBusApp') }}
	WHERE svIsNullSmCaseId = 'N' AND svIsNullSubTypeCode = 'N' AND svIsNullAppId = 'N'
)

SELECT * FROM Transfrm