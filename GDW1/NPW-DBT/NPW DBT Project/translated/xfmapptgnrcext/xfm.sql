{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH Xfm AS (
	SELECT
		-- *SRC*: \(20)If IsNull(frmlkp.DATE_ROLE_C) OR Trim(( IF IsNotNull((frmlkp.DATE_ROLE_C)) THEN (frmlkp.DATE_ROLE_C) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('LkpDateRoleC') }}.DATE_ROLE_C IS NULL OR TRIM(IFF({{ ref('LkpDateRoleC') }}.DATE_ROLE_C IS NOT NULL, {{ ref('LkpDateRoleC') }}.DATE_ROLE_C, '')) = '', 'N', 'Y') AS svlsNullDateRoleC,
		-- *SRC*: \(20)If IsNull(frmlkp.audit_date) OR frmlkp.audit_date = '0' OR Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('LkpDateRoleC') }}.audit_date IS NULL OR {{ ref('LkpDateRoleC') }}.audit_date = '0' OR TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, '')) = '', 'N', 'Y') AS svIsNullAuditDate,
		-- *SRC*: \(20)If svIsNullAuditDate = 'Y' and IsValid('date', Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")[7, 2])) Then 'Y' else 'N',
		IFF(    
	    svIsNullAuditDate = 'Y'
	    and ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, '')), '-'), TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, ''))), '-'), TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, '')))), 
	    'Y', 
	    'N'
	) AS svIsValiduditDateD,
		-- *SRC*: \(20)If svIsNullAuditDate = 'Y' and IsValid('time', Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")[10, 2]) : ':' : Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")[13, 2]) : ':' : Trim(( IF IsNotNull((frmlkp.audit_date)) THEN (frmlkp.audit_date) ELSE "")[16, 2])) Then 'Y' else 'N',
		IFF(    
	    svIsNullAuditDate = 'Y'
	    and ISVALID('time', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, '')), ':'), TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, ''))), ':'), TRIM(IFF({{ ref('LkpDateRoleC') }}.audit_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.audit_date, '')))), 
	    'Y', 
	    'N'
	) AS svIsValiduditDateT,
		-- *SRC*: \(20)If IsNull(frmlkp.delivery_date) Or frmlkp.delivery_date = '0' Or Trim(( IF IsNotNull((frmlkp.delivery_date)) THEN (frmlkp.delivery_date) ELSE "")) = '' then 'N' Else 'Y',
		IFF({{ ref('LkpDateRoleC') }}.delivery_date IS NULL OR {{ ref('LkpDateRoleC') }}.delivery_date = '0' OR TRIM(IFF({{ ref('LkpDateRoleC') }}.delivery_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.delivery_date, '')) = '', 'N', 'Y') AS svIsNullDeliveryDate,
		-- *SRC*: \(20)If svIsNullDeliveryDate = 'Y' And IsValid('date', Trim(( IF IsNotNull((frmlkp.delivery_date)) THEN (frmlkp.delivery_date) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((frmlkp.delivery_date)) THEN (frmlkp.delivery_date) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((frmlkp.delivery_date)) THEN (frmlkp.delivery_date) ELSE "")[7, 2])) Then 'Y' Else 'N',
		IFF(    
	    svIsNullDeliveryDate = 'Y'
	    and ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('LkpDateRoleC') }}.delivery_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.delivery_date, '')), '-'), TRIM(IFF({{ ref('LkpDateRoleC') }}.delivery_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.delivery_date, ''))), '-'), TRIM(IFF({{ ref('LkpDateRoleC') }}.delivery_date IS NOT NULL, {{ ref('LkpDateRoleC') }}.delivery_date, '')))), 
	    'Y', 
	    'N'
	) AS svIsValidDeliveryDate,
		-- *SRC*: \(20)If IsNull(frmlkp.mod_user_id) OR Trim(( IF IsNotNull((frmlkp.mod_user_id)) THEN (frmlkp.mod_user_id) ELSE "")) = '' then 'N' Else 'Y',
		IFF({{ ref('LkpDateRoleC') }}.mod_user_id IS NULL OR TRIM(IFF({{ ref('LkpDateRoleC') }}.mod_user_id IS NOT NULL, {{ ref('LkpDateRoleC') }}.mod_user_id, '')) = '', 'N', 'Y') AS svIsNullModUserId,
		-- *SRC*: \(20)If IsNull(frmlkp.change_cat_id) OR Trim(( IF IsNotNull((frmlkp.change_cat_id)) THEN (frmlkp.change_cat_id) ELSE "")) = '' then 'Y' Else 'N',
		IFF({{ ref('LkpDateRoleC') }}.change_cat_id IS NULL OR TRIM(IFF({{ ref('LkpDateRoleC') }}.change_cat_id IS NOT NULL, {{ ref('LkpDateRoleC') }}.change_cat_id, '')) = '', 'Y', 'N') AS svIsnullCheck,
		-- *SRC*: \(20)If frmlkp.promise_type = '3' then 'AFUN' Else  If svIsnullCheck = 'Y' then 'PR01' Else 'Y',
		IFF({{ ref('LkpDateRoleC') }}.promise_type = '3', 'AFUN', IFF(svIsnullCheck = 'Y', 'PR01', 'Y')) AS svIsNulChangeCatId,
		-- *SRC*: 'CSE' : 'CL' : Trim(frmlkp.ccl_app_id),
		CONCAT(CONCAT('CSE', 'CL'), TRIM({{ ref('LkpDateRoleC') }}.ccl_app_id)) AS APPT_I,
		-- *SRC*: \(20)If svlsNullDateRoleC = 'Y' Then Trim(frmlkp.DATE_ROLE_C) Else '9999',
		IFF(svlsNullDateRoleC = 'Y', TRIM({{ ref('LkpDateRoleC') }}.DATE_ROLE_C), '9999') AS DATE_ROLE_C,
		-- *SRC*: \(20)If svIsNullAuditDate = 'N' then SetNull() ELSE  If (svIsValiduditDateD = 'Y' And svIsValiduditDateT = 'Y') Then StringToTimestamp(Trim(frmlkp.audit_date[1, 4]) : '-' : Trim(frmlkp.audit_date[5, 2]) : '-' : Trim(frmlkp.audit_date[7, 2]) : ' ' : Trim(frmlkp.audit_date[10, 2]) : ':' : Trim(frmlkp.audit_date[13, 2]) : ':' : Trim(frmlkp.audit_date[16, 2])) ELSE  If (svIsValiduditDateD = 'Y' And svIsValiduditDateT = 'N') Then StringToTimestamp(Trim(frmlkp.audit_date[1, 4]) : '-' : Trim(frmlkp.audit_date[5, 2]) : '-' : Trim(frmlkp.audit_date[7, 2]) : ' ' : '00' : ':' : '00' : ':' : '00', "%yyyy-%mm-%dd %hh:%nn:%ss") Else  if (svIsValiduditDateD = 'N' And svIsValiduditDateT = 'Y') Then StringToTimestamp('1111' : '-' : '11' : '-' : '11' : ' ' : Trim(frmlkp.audit_date[10, 2]) : ':' : Trim(frmlkp.audit_date[13, 2]) : ':' : Trim(frmlkp.audit_date[16, 2]), "%yyyy-%mm-%dd %hh:%nn:%ss") else StringToTimestamp('1111' : '-' : '11' : '-' : '11' : ' ' : '00' : ':' : '00' : ':' : '00', "%yyyy-%mm-%dd %hh:%nn:%ss"),
		IFF(
	    svIsNullAuditDate = 'N', SETNULL(),     
	    IFF(
	        svIsValiduditDateD = 'Y'
	    and svIsValiduditDateT = 'Y', 
	        STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 7, 2))), ' '), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 10, 2))), ':'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 13, 2))), ':'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 16, 2)))),         
	        IFF(
	            svIsValiduditDateD = 'Y'
	        and svIsValiduditDateT = 'N', 
	            STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 7, 2))), ' '), '00'), ':'), '00'), ':'), '00'), '%yyyy-%mm-%dd %hh:%nn:%ss'),             
	            IFF(
	                svIsValiduditDateD = 'N'
	            and svIsValiduditDateT = 'Y', 
	                STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('1111', '-'), '11'), '-'), '11'), ' '), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 10, 2))), ':'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 13, 2))), ':'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 16, 2))), '%yyyy-%mm-%dd %hh:%nn:%ss'), 
	                STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('1111', '-'), '11'), '-'), '11'), ' '), '00'), ':'), '00'), ':'), '00'), '%yyyy-%mm-%dd %hh:%nn:%ss')
	            )
	        )
	    )
	) AS MODF_S,
		-- *SRC*: \(20)If svIsNullAuditDate = 'N' then SetNull() ELSE  If svIsValiduditDateD = 'Y' Then StringToDate(Trim(frmlkp.audit_date[1, 4]) : '-' : Trim(frmlkp.audit_date[5, 2]) : '-' : Trim(frmlkp.audit_date[7, 2]), "%yyyy-%mm-%dd") else StringToDate(1111 : '-' : 11 : '-' : 11, "%yyyy-%mm-%dd"),
		IFF(
	    svIsNullAuditDate = 'N', SETNULL(),     
	    IFF(
	        svIsValiduditDateD = 'Y', STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 7, 2))), '%yyyy-%mm-%dd'), 
	        STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(1111, '-'), 11), '-'), 11), '%yyyy-%mm-%dd')
	    )
	) AS MODF_D,
		-- *SRC*: \(20)If svIsNullAuditDate = 'N' then SetNull() ELSE  If svIsValiduditDateT = 'Y' Then StringToTime(Trim(frmlkp.audit_date[10, 2]) : ':' : Trim(frmlkp.audit_date[13, 2]) : ':' : Trim(frmlkp.audit_date[16, 2]), "%hh:%nn:%ss") else '00:00:00',
		IFF(
	    svIsNullAuditDate = 'N', SETNULL(), 
	    IFF(svIsValiduditDateT = 'Y', STRINGTOTIME(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 10, 2)), ':'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 13, 2))), ':'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.audit_date, 16, 2))), '%hh:%nn:%ss'), '00:00:00')
	) AS MODF_T,
		-- *SRC*: \(20)If svIsNullDeliveryDate = 'N' then setNull() Else  If svIsValidDeliveryDate = 'N' then '1111-11-11 00:00:00' else StringToTimestamp(Trim(frmlkp.delivery_date[1, 4]) : '-' : Trim(frmlkp.delivery_date[5, 2]) : '-' : Trim(frmlkp.delivery_date[7, 2]) : ' ' : '00:00:00'),
		IFF(
	    svIsNullDeliveryDate = 'N', SETNULL(),     
	    IFF(
	        svIsValidDeliveryDate = 'N', '1111-11-11 00:00:00', 
	        STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.delivery_date, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.delivery_date, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.delivery_date, 7, 2))), ' '), '00:00:00'))
	    )
	) AS GNRC_ROLE_S,
		-- *SRC*: \(20)If svIsNullDeliveryDate = 'N' then setNull() Else  If svIsValidDeliveryDate = 'N' then StringToDate('1111-11-11', "%yyyy-%mm-%dd") else StringToDate(Trim(frmlkp.delivery_date[1, 4]) : '-' : Trim(frmlkp.delivery_date[5, 2]) : '-' : Trim(frmlkp.delivery_date[7, 2]), "%yyyy-%mm-%dd"),
		IFF(
	    svIsNullDeliveryDate = 'N', SETNULL(),     
	    IFF(
	        svIsValidDeliveryDate = 'N', STRINGTODATE('1111-11-11', '%yyyy-%mm-%dd'), 
	        STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.delivery_date, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.delivery_date, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('LkpDateRoleC') }}.delivery_date, 7, 2))), '%yyyy-%mm-%dd')
	    )
	) AS GNRC_ROLE_D,
		-- *SRC*: SetNull(),
		SETNULL() AS GNRC_ROLE_T,
		-- *SRC*: \(20)If svIsNullModUserId = 'N' then SetNull() Else 'CSE' : 'C1' : Trim(frmlkp.mod_user_id),
		IFF(svIsNullModUserId = 'N', SETNULL(), CONCAT(CONCAT('CSE', 'C1'), TRIM({{ ref('LkpDateRoleC') }}.mod_user_id))) AS USER_I,
		svIsNulChangeCatId AS CHNG_REAS_TYPE_C,
		promise_type,
		-- *SRC*: \(20)If IsNull(frmlkp.change_cat_id) then SetNull() Else frmlkp.change_cat_id,
		IFF({{ ref('LkpDateRoleC') }}.change_cat_id IS NULL, SETNULL(), {{ ref('LkpDateRoleC') }}.change_cat_id) AS change_cat_id,
		ccl_app_id,
		audit_date,
		delivery_date
	FROM {{ ref('LkpDateRoleC') }}
	WHERE 
)

SELECT * FROM Xfm