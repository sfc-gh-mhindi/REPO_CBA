{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH TransXfm AS (
	SELECT
		-- *SRC*: \(20)If IsNull(JoinUnique.APP_ID) Then 'Y' Else  if Trim(JoinUnique.APP_ID) = 0 Then 'Y' Else  if Trim(( IF IsNotNull((JoinUnique.APP_ID)) THEN (JoinUnique.APP_ID) ELSE "")) = '' Then 'Y' Else 'N',
		IFF({{ ref('Join_Sm_Case_Id') }}.APP_ID IS NULL, 'Y', IFF(TRIM({{ ref('Join_Sm_Case_Id') }}.APP_ID) = 0, 'Y', IFF(TRIM(IFF({{ ref('Join_Sm_Case_Id') }}.APP_ID IS NOT NULL, {{ ref('Join_Sm_Case_Id') }}.APP_ID, '')) = '', 'Y', 'N'))) AS svIsNullAppId,
		APP_ID,
		SM_STATE_CAT_ID,
		START_D,
		END_D,
		CREATED_BY_STAFF_NUMBER,
		SUBTYPE_CODE
	FROM {{ ref('Join_Sm_Case_Id') }}
	WHERE svIsNullAppId = 'N'
)

SELECT * FROM TransXfm