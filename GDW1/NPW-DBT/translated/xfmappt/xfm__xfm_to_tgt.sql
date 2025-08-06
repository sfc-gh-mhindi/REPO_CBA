{{ config(materialized='view', tags=['XfmAppt']) }}

WITH Xfm__Xfm_to_Tgt AS (
	SELECT
		-- *SRC*: \(20)If IsValid('date', Trim(( IF IsNotNull((ToXfm.CREATED_DATE)) THEN (ToXfm.CREATED_DATE) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((ToXfm.CREATED_DATE)) THEN (ToXfm.CREATED_DATE) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((ToXfm.CREATED_DATE)) THEN (ToXfm.CREATED_DATE) ELSE "")[7, 2])) Then 'Y' Else 'N',
		IFF(ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('FnlToXfm') }}.CREATED_DATE IS NOT NULL, {{ ref('FnlToXfm') }}.CREATED_DATE, '')), '-'), TRIM(IFF({{ ref('FnlToXfm') }}.CREATED_DATE IS NOT NULL, {{ ref('FnlToXfm') }}.CREATED_DATE, ''))), '-'), TRIM(IFF({{ ref('FnlToXfm') }}.CREATED_DATE IS NOT NULL, {{ ref('FnlToXfm') }}.CREATED_DATE, '')))), 'Y', 'N') AS svIsValidCreatedDate,
		-- *SRC*: \(20)If IsNull(ToXfm.CHANNEL_CAT_ID) Then 'Y' Else  If ( IF IsNotNull((ToXfm.CHANNEL_CAT_ID)) THEN (ToXfm.CHANNEL_CAT_ID) ELSE "") = '' Then 'Y' Else  if Trim(ToXfm.CHANNEL_CAT_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('FnlToXfm') }}.CHANNEL_CAT_ID IS NULL, 'Y', IFF(IFF({{ ref('FnlToXfm') }}.CHANNEL_CAT_ID IS NOT NULL, {{ ref('FnlToXfm') }}.CHANNEL_CAT_ID, '') = '', 'Y', IFF(TRIM({{ ref('FnlToXfm') }}.CHANNEL_CAT_ID) = 0, 'Y', 'N'))) AS svIsNullChannelCatId,
		-- *SRC*: \(20)If IsNull(ToXfm.APPT_ORIG_C) Then 'Y' Else  If Trim(ToXfm.APPT_ORIG_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('FnlToXfm') }}.APPT_ORIG_C IS NULL, 'Y', IFF(TRIM({{ ref('FnlToXfm') }}.APPT_ORIG_C) = '', 'Y', 'N')) AS svIsNullApptOrigC,
		-- *SRC*: "CSEPO" : Trim(ToXfm.APP_ID),
		CONCAT('CSEPO', TRIM({{ ref('FnlToXfm') }}.APP_ID)) AS APPT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS APPT_C,
		-- *SRC*: SetNull(),
		SETNULL() AS APPT_FORM_C,
		'PO' AS APPT_QLFY_C,
		-- *SRC*: SetNull(),
		SETNULL() AS STUS_TRAK_I,
		-- *SRC*: \(20)If svIsNullChannelCatId = 'Y' Then SetNull() Else  if svIsNullApptOrigC = 'Y' Then '9999' Else ToXfm.APPT_ORIG_C,
		IFF(svIsNullChannelCatId = 'Y', SETNULL(), IFF(svIsNullApptOrigC = 'Y', '9999', {{ ref('FnlToXfm') }}.APPT_ORIG_C)) AS APPT_ORIG_C,
		{{ ref('FnlToXfm') }}.APP_NO AS APPT_N,
		'CSE' AS SRCE_SYST_C,
		{{ ref('FnlToXfm') }}.APP_ID AS SRCE_SYST_APPT_I,
		-- *SRC*: \(20)If svIsValidCreatedDate = 'Y' Then StringToDate(Trim(ToXfm.CREATED_DATE[1, 4]) : '-' : Trim(ToXfm.CREATED_DATE[5, 2]) : '-' : Trim(ToXfm.CREATED_DATE[7, 2]), "%yyyy-%mm-%dd") Else pGDW_DEFAULT_DATE,
		IFF(svIsValidCreatedDate = 'Y', STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING({{ ref('FnlToXfm') }}.CREATED_DATE, 1, 4)), '-'), TRIM(SUBSTRING({{ ref('FnlToXfm') }}.CREATED_DATE, 5, 2))), '-'), TRIM(SUBSTRING({{ ref('FnlToXfm') }}.CREATED_DATE, 7, 2))), '%yyyy-%mm-%dd'), pGDW_DEFAULT_DATE) AS APPT_CRAT_D,
		-- *SRC*: SetNull(),
		SETNULL() AS RATE_SEEK_F,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		pRUN_STRM_C AS RUN_STRM,
		-- *SRC*: SetNull(),
		SETNULL() AS ORIG_APPT_SRCE_C,
		-- *SRC*: SetNull(),
		SETNULL() AS APPT_RECV_S,
		-- *SRC*: SetNull(),
		SETNULL() AS REL_MGR_STAT_C,
		-- *SRC*: SetNull(),
		SETNULL() AS APPT_RECV_D,
		-- *SRC*: SetNull(),
		SETNULL() AS APPT_RECV_T,
		{{ ref('FnlToXfm') }}.APP_ENTRY_POINT AS APPT_ENTR_POIT_M
	FROM {{ ref('FnlToXfm') }}
	WHERE 
)

SELECT * FROM Xfm__Xfm_to_Tgt