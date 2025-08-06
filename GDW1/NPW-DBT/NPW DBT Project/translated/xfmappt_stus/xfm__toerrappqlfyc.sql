{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH Xfm__ToErrAppQlfyC AS (
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
		-- *SRC*: trim(ToXfm.APP_ID),
		TRIM({{ ref('Lkp_SM_CASE_STUS') }}.APP_ID) AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'Lookup Failure on MAP_CSE_APPT_QLFY' AS CONV_MAP_RULE_M,
		'APPT_STUS' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('Lkp_SM_CASE_STUS') }}.SUBTYPE_CODE AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'APPT_I' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_COM_BUS_APP' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSE" : 9999 : Trim(ToXfm.APP_ID),
		CONCAT(CONCAT('CSE', 9999), TRIM({{ ref('Lkp_SM_CASE_STUS') }}.APP_ID)) AS TRSF_KEY_I
	FROM {{ ref('Lkp_SM_CASE_STUS') }}
	WHERE svIsNullApptQlfyC = 'Y'
)

SELECT * FROM Xfm__ToErrAppQlfyC