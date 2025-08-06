{{ config(materialized='view', tags=['XfmAppt_Empl']) }}

WITH Xfm__ToErr AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToLkp.APPT_QLFY_C) Then 'Y' Else  If Trim(ToLkp.APPT_QLFY_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM({{ ref('LkpApptQlfyC') }}.APPT_QLFY_C) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: \(20)if IsNull(ToLkp.OWNED_BY_STAFF_NUMBER) Or ToLkp.OWNED_BY_STAFF_NUMBER = 0 or trim(( IF IsNotNull((ToLkp.OWNED_BY_STAFF_NUMBER)) THEN (ToLkp.OWNED_BY_STAFF_NUMBER) ELSE "")) = '' then 'Y' ELSE 'N',
		IFF({{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER IS NULL OR {{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER = 0 OR TRIM(IFF({{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('LkpApptQlfyC') }}.OWNED_BY_STAFF_NUMBER, '')) = '', 'Y', 'N') AS svIsNullOwnedByStaffNumber,
		-- *SRC*: \(20)if IsNull(ToLkp.CREATED_BY_STAFF_NUMBER) Or ToLkp.CREATED_BY_STAFF_NUMBER = 0 or trim(( IF IsNotNull((ToLkp.CREATED_BY_STAFF_NUMBER)) THEN (ToLkp.CREATED_BY_STAFF_NUMBER) ELSE "")) = '' then 'Y' ELSE 'N',
		IFF({{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER IS NULL OR {{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER = 0 OR TRIM(IFF({{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('LkpApptQlfyC') }}.CREATED_BY_STAFF_NUMBER, '')) = '', 'Y', 'N') AS svIsNullCreatedByStaffNumber,
		{{ ref('LkpApptQlfyC') }}.APP_ID AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_APPT_QLFY' AS CONV_MAP_RULE_M,
		'APPT_EMPL' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('LkpApptQlfyC') }}.SUBTYPE_CODE AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'APPT_QLFY_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_COM_BUS_APP' AS SRCE_FILE_M,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSE" : 99999 : ToLkp.APP_ID,
		CONCAT(CONCAT('CSE', 99999), {{ ref('LkpApptQlfyC') }}.APP_ID) AS TRSF_KEY_I
	FROM {{ ref('LkpApptQlfyC') }}
	WHERE svIsNullApptQlfyC = 'Y' AND svIsNullCreatedByStaffNumber = 'N' OR svIsNullApptQlfyC = 'Y' AND svIsNullOwnedByStaffNumber = 'N'
)

SELECT * FROM Xfm__ToErr