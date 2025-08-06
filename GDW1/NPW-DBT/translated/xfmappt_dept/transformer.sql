{{ config(materialized='view', tags=['XfmAppt_Dept']) }}

WITH Transformer AS (
	SELECT
		-- *SRC*: \(20)if FrmCpy.SUBTYPE_CODE <> 'PO' THEN 'N' ELSE 'Y',
		IFF({{ ref('Cpy') }}.SUBTYPE_CODE <> 'PO', 'N', 'Y') AS svIsSubTypeCode,
		-- *SRC*: \(20)If IsNull(FrmCpy.APP_ID) Then 'Y' Else  If Trim(FrmCpy.APP_ID) = '' Then 'Y' Else  If Trim(FrmCpy.APP_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Cpy') }}.APP_ID IS NULL, 'Y', IFF(TRIM({{ ref('Cpy') }}.APP_ID) = '', 'Y', IFF(TRIM({{ ref('Cpy') }}.APP_ID) = 0, 'Y', 'N'))) AS svIsNullAppId,
		-- *SRC*: \(20)If IsNull(FrmCpy.GL_DEPT_NO) Then 'Y' Else  If Trim(FrmCpy.GL_DEPT_NO) = '' Then 'Y' Else  If Trim(FrmCpy.GL_DEPT_NO) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Cpy') }}.GL_DEPT_NO IS NULL, 'Y', IFF(TRIM({{ ref('Cpy') }}.GL_DEPT_NO) = '', 'Y', IFF(TRIM({{ ref('Cpy') }}.GL_DEPT_NO) = 0, 'Y', 'N'))) AS svIsNullGlDeptNo,
		SUBTYPE_CODE,
		APP_ID,
		GL_DEPT_NO
	FROM {{ ref('Cpy') }}
	WHERE svIsSubTypeCode = 'Y' AND svIsNullAppId = 'N' AND svIsNullGlDeptNo = 'N'
)

SELECT * FROM Transformer