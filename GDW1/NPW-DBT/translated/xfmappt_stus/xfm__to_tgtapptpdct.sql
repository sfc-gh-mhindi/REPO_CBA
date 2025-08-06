{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH Xfm__To_TgtApptPdct AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.APPT_QLFY_C) Then 'Y' Else  If Trim(( IF IsNotNull((ToXfm.APPT_QLFY_C)) THEN (ToXfm.APPT_QLFY_C) ELSE "")) = '' Then 'Y' Else 'N',
		IFF({{ ref('Lkp_SM_CASE_STUS') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.APPT_QLFY_C IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.APPT_QLFY_C, '')) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: \(20)If IsNull(ToXfm.START_D) OR ToXfm.START_D = '0' OR Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NULL OR {{ ref('Lkp_SM_CASE_STUS') }}.START_D = '0' OR TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, '')) = '', 'N', 'Y') AS svIsNullStrtD,
		-- *SRC*: \(20)If IsNull(ToXfm.END_D) then 'N' ELSE  IF ToXfm.END_D = '0' THEN 'N' ELSE  IF Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NULL, 'N', IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D = '0', 'N', IFF(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, '')) = '', 'N', 'Y'))) AS svIsNullEndD,
		-- *SRC*: \(20)If IsNull(ToXfm.STUS_C) Then 'Y' Else  If TRIM(( IF IsNotNull((ToXfm.STUS_C)) THEN (ToXfm.STUS_C) ELSE "")) = '' Then 'Y' Else 'N',
		IFF({{ ref('Lkp_SM_CASE_STUS') }}.STUS_C IS NULL, 'Y', IFF(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.STUS_C IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.STUS_C, '')) = '', 'Y', 'N')) AS svIsNullStusC,
		-- *SRC*: \(20)If svIsNullStrtD = 'Y' and IsValid('date', Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")[7, 2])) Then 'Y' else 'N',
		IFF(    
	    svIsNullStrtD = 'Y'
	    and ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, '')), '-'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, ''))), '-'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, '')))), 
	    'Y', 
	    'N'
	) AS svIsValidStrtD,
		-- *SRC*: \(20)If svIsNullEndD = 'Y' and IsValid('date', Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")[7, 2])) Then 'Y' else 'N',
		IFF(    
	    svIsNullEndD = 'Y'
	    and ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, '')), '-'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, ''))), '-'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, '')))), 
	    'Y', 
	    'N'
	) AS svIsValidEndD,
		-- *SRC*: \(20)If svIsNullStrtD = 'Y' and IsValid('time', Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")[9, 2]) : ':' : Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")[11, 2]) : ':' : Trim(( IF IsNotNull((ToXfm.START_D)) THEN (ToXfm.START_D) ELSE "")[13, 2])) Then 'Y' else 'N',
		IFF(    
	    svIsNullStrtD = 'Y'
	    and ISVALID('time', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, '')), ':'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, ''))), ':'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.START_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.START_D, '')))), 
	    'Y', 
	    'N'
	) AS svIsValidStrtT,
		-- *SRC*: \(20)If svIsNullEndD = 'Y' and IsValid('time', Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")[9, 2]) : ':' : Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")[11, 2]) : ':' : Trim(( IF IsNotNull((ToXfm.END_D)) THEN (ToXfm.END_D) ELSE "")[13, 2])) Then 'Y' else 'N',
		IFF(    
	    svIsNullEndD = 'Y'
	    and ISVALID('time', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, '')), ':'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, ''))), ':'), TRIM(IFF({{ ref('Lkp_SM_CASE_STUS') }}.END_D IS NOT NULL, {{ ref('Lkp_SM_CASE_STUS') }}.END_D, '')))), 
	    'Y', 
	    'N'
	) AS svIsValidEndT,
		-- *SRC*: "CSE" : ( If svIsNullApptQlfyC = 'Y' Then 9999 Else ToXfm.APPT_QLFY_C) : ToXfm.APP_ID,
		CONCAT(CONCAT('CSE', IFF(svIsNullApptQlfyC = 'Y', 9999, {{ ref('Lkp_SM_CASE_STUS') }}.APPT_QLFY_C)), {{ ref('Lkp_SM_CASE_STUS') }}.APP_ID) AS APPT_I,
		-- *SRC*: \(20)if svIsNullStusC = 'N' then ToXfm.STUS_C else '9999',
		IFF(svIsNullStusC = 'N', {{ ref('Lkp_SM_CASE_STUS') }}.STUS_C, '9999') AS STUS_C,
		-- *SRC*: \(20)If svIsValidStrtD = 'Y' Then StringToDate(Trim(ToXfm.START_D[1, 4]) : '-' : Trim(ToXfm.START_D[5, 2]) : '-' : Trim(ToXfm.START_D[7, 2]), "%yyyy-%mm-%dd") else StringToDate(1111 : '-' : 11 : '-' : 11, "%yyyy-%mm-%dd"),
		IFF(
	    svIsValidStrtD = 'Y', STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 7, 2))), '%yyyy-%mm-%dd'), 
	    STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(1111, '-'), 11), '-'), 11), '%yyyy-%mm-%dd')
	) AS STRT_D,
		-- *SRC*: \(20)If svIsValidStrtT = 'Y' Then StringToTime(Trim(ToXfm.START_D[9, 2]) : ':' : Trim(ToXfm.START_D[11, 2]) : ':' : Trim(ToXfm.START_D[13, 2]), "%hh:%nn:%ss") else '00:00:00',
		IFF(svIsValidStrtT = 'Y', STRINGTOTIME(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 9, 2)), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 11, 2))), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 13, 2))), '%hh:%nn:%ss'), '00:00:00') AS STRT_T,
		-- *SRC*: \(20)If (svIsValidStrtD = 'Y' And svIsValidStrtT = 'Y') Then StringToTimestamp(Trim(ToXfm.START_D[1, 4]) : '-' : Trim(ToXfm.START_D[5, 2]) : '-' : Trim(ToXfm.START_D[7, 2]) : ' ' : Trim(ToXfm.START_D[9, 2]) : ':' : Trim(ToXfm.START_D[11, 2]) : ':' : Trim(ToXfm.START_D[13, 2])) else StringToTimestamp('1111' : '-' : '11' : '-' : '11' : ' ' : '00' : ':' : '00' : ':' : '00', "%yyyy-%mm-%dd %hh:%nn:%ss"),
		IFF(
	    svIsValidStrtD = 'Y' AND svIsValidStrtT = 'Y', 
	    STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 7, 2))), ' '), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 9, 2))), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 11, 2))), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.START_D, 13, 2)))), 
	    STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('1111', '-'), '11'), '-'), '11'), ' '), '00'), ':'), '00'), ':'), '00'), '%yyyy-%mm-%dd %hh:%nn:%ss')
	) AS STRT_S,
		-- *SRC*: \(20)If svIsNullEndD = 'N' THEN SetNull() Else  If svIsValidEndD = 'Y' Then StringToDate(Trim(ToXfm.END_D[1, 4]) : '-' : Trim(ToXfm.END_D[5, 2]) : '-' : Trim(ToXfm.END_D[7, 2]), "%yyyy-%mm-%dd") else StringToDate(1111 : '-' : 11 : '-' : 11, "%yyyy-%mm-%dd"),
		IFF(
	    svIsNullEndD = 'N', SETNULL(),     
	    IFF(
	        svIsValidEndD = 'Y', STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 7, 2))), '%yyyy-%mm-%dd'), 
	        STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(1111, '-'), 11), '-'), 11), '%yyyy-%mm-%dd')
	    )
	) AS END_D,
		-- *SRC*: \(20)If svIsNullEndD = 'N' THEN SetNull() Else  If svIsValidEndT = 'Y' Then StringToTime(Trim(ToXfm.END_D[9, 2]) : ':' : Trim(ToXfm.END_D[11, 2]) : ':' : Trim(ToXfm.END_D[13, 2]), "%hh:%nn:%ss") else '00:00:00',
		IFF(svIsNullEndD = 'N', SETNULL(), IFF(svIsValidEndT = 'Y', STRINGTOTIME(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 9, 2)), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 11, 2))), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 13, 2))), '%hh:%nn:%ss'), '00:00:00')) AS END_T,
		-- *SRC*: \(20)If svIsNullEndD = 'N' THEN SetNull() Else  If (svIsValidEndD = 'Y' And svIsValidEndT = 'Y') Then StringToTimestamp(Trim(ToXfm.END_D[1, 4]) : '-' : Trim(ToXfm.END_D[5, 2]) : '-' : Trim(ToXfm.END_D[7, 2]) : ' ' : Trim(ToXfm.END_D[9, 2]) : ':' : Trim(ToXfm.END_D[11, 2]) : ':' : Trim(ToXfm.END_D[13, 2])) else StringToTimestamp('1111' : '-' : '11' : '-' : '11' : ' ' : '00' : ':' : '00' : ':' : '00', "%yyyy-%mm-%dd %hh:%nn:%ss"),
		IFF(
	    svIsNullEndD = 'N', SETNULL(),     
	    IFF(
	        svIsValidEndD = 'Y'
	    and svIsValidEndT = 'Y', 
	        STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 7, 2))), ' '), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 9, 2))), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 11, 2))), ':'), TRIM(SUBSTRING({{ ref('Lkp_SM_CASE_STUS') }}.END_D, 13, 2)))), 
	        STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('1111', '-'), '11'), '-'), '11'), ' '), '00'), ':'), '00'), ':'), '00'), '%yyyy-%mm-%dd %hh:%nn:%ss')
	    )
	) AS END_S,
		{{ ref('Lkp_SM_CASE_STUS') }}.CREATED_BY_STAFF_NUMBER AS EMPL_I,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		pRUN_STRM_C AS RUN_STRM,
		SM_STATE_CAT_ID
	FROM {{ ref('Lkp_SM_CASE_STUS') }}
	WHERE 
)

SELECT * FROM Xfm__To_TgtApptPdct