{{ config(materialized='view', tags=['XfmAppt_Empl']) }}

WITH Xfm__Xfm_to_Tgt_own AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToLkp.APPT_QLFY_C) Then 'Y' Else  If Trim(ToLkp.APPT_QLFY_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: \(20)if IsNull(ToLkp.OWNED_BY_STAFF_NUMBER) Or ToLkp.OWNED_BY_STAFF_NUMBER = 0 or trim(( IF IsNotNull((ToLkp.OWNED_BY_STAFF_NUMBER)) THEN (ToLkp.OWNED_BY_STAFF_NUMBER) ELSE "")) = '' then 'Y' ELSE 'N',
		IFF({{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER IS NULL OR {{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER = 0 OR TRIM(IFF({{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER, '')) = '', 'Y', 'N') AS svIsNullOwnedByStaffNumber,
		-- *SRC*: \(20)if IsNull(ToLkp.CREATED_BY_STAFF_NUMBER) Or ToLkp.CREATED_BY_STAFF_NUMBER = 0 or trim(( IF IsNotNull((ToLkp.CREATED_BY_STAFF_NUMBER)) THEN (ToLkp.CREATED_BY_STAFF_NUMBER) ELSE "")) = '' then 'Y' ELSE 'N',
		IFF({{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER IS NULL OR {{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER = 0 OR TRIM(IFF({{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER, '')) = '', 'Y', 'N') AS svIsNullCreatedByStaffNumber,
		-- *SRC*: "CSE" : ( If svIsNullApptQlfyC = 'Y' Then "9999" Else Trim(ToLkp.APPT_QLFY_C)) : Trim(ToLkp.APP_ID),
		CONCAT(CONCAT('CSE', IFF(svIsNullApptQlfyC = 'Y', '9999', TRIM({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C))), TRIM({{ ref('LkpApptQlfyC') }}.APP_ID)) AS APPT_I,
		{{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER AS EMPL_I,
		'OWN' AS EMPL_ROLE_C,
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
	WHERE svIsNullOwnedByStaffNumber = 'N'
)

SELECT * FROM Xfm__Xfm_to_Tgt_own