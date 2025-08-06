{{ config(materialized='view', tags=['XfmAppt_Dept']) }}

WITH Xfm__Xfm_to_Tgt AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToLkp.APPT_QLFY_C) Then 'Y' Else  If Trim(ToLkp.APPT_QLFY_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: "CSE" : ( If svIsNullApptQlfyC = 'Y' Then "9999" Else Trim(ToLkp.APPT_QLFY_C)) : ToLkp.APP_ID,
		CONCAT(CONCAT('CSE', IFF(svIsNullApptQlfyC = 'Y', '9999', TRIM({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C))), {{ ref('LkpApptQlfyC') }}.APP_ID) AS APPT_I,
		{{ ref('LkpApptQlfyC') }}.GL_DEPT_NO AS DEPT_I,
		'LDGT' AS DEPT_ROLE_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('LkpApptQlfyC') }}
	WHERE 
)

SELECT * FROM Xfm__Xfm_to_Tgt