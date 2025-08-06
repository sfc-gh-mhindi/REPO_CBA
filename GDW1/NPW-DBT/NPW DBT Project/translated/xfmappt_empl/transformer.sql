{{ config(materialized='view', tags=['XfmAppt_Empl']) }}

WITH Transformer AS (
	SELECT
		-- *SRC*: \(20)if FrmCpy.SUBTYPE_CODE <> 'PO' THEN 'N' ELSE 'Y',
		IFF({{ ref('Cpy') }}.SUBTYPE_CODE <> 'PO', 'N', 'Y') AS svIsSubTypeCode,
		-- *SRC*: \(20)If IsNull(FrmCpy.APP_ID) Then 'Y' Else  If Trim(FrmCpy.APP_ID) = '' Then 'Y' Else  If Trim(FrmCpy.APP_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Cpy') }}.APP_ID IS NULL, 'Y', IFF(TRIM({{ ref('Cpy') }}.APP_ID) = '', 'Y', IFF(TRIM({{ ref('Cpy') }}.APP_ID) = 0, 'Y', 'N'))) AS svIsNullAppId,
		SUBTYPE_CODE,
		APP_ID,
		CREATED_BY_STAFF_NUMBER,
		OWNED_BY_STAFF_NUMBER
	FROM {{ ref('Cpy') }}
	WHERE svIsSubTypeCode = 'Y' AND svIsNullAppId = 'N'
)

SELECT * FROM Transformer