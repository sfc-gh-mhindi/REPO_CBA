{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH DropNullRec AS (
	SELECT
		-- *SRC*: \(20)If IsNull(NullRec.WIM_PROCESS_ID) Then 'Y' Else  If Trim(NullRec.WIM_PROCESS_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('Rem_Dup') }}.WIM_PROCESS_ID IS NULL, 'Y', IFF(TRIM({{ ref('Rem_Dup') }}.WIM_PROCESS_ID) = '', 'Y', 'N')) AS svNullWimProcessId,
		MOD_TIMESTAMP,
		OL_CLIENT_RM_RATING_ID,
		CLIENT_ID,
		CIF_CODE,
		OU_ID,
		CS_USER_ID,
		RATING,
		WIM_PROCESS_ID
	FROM {{ ref('Rem_Dup') }}
	WHERE svNullWimProcessId = 'N'
)

SELECT * FROM DropNullRec