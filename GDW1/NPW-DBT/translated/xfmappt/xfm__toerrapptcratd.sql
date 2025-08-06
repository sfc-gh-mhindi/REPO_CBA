{{ config(materialized='view', tags=['XfmAppt']) }}

WITH Xfm__ToErrApptCratD AS (
	SELECT
		-- *SRC*: \(20)If IsValid('date', Trim(( IF IsNotNull((ToXfm.CREATED_DATE)) THEN (ToXfm.CREATED_DATE) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((ToXfm.CREATED_DATE)) THEN (ToXfm.CREATED_DATE) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((ToXfm.CREATED_DATE)) THEN (ToXfm.CREATED_DATE) ELSE "")[7, 2])) Then 'Y' Else 'N',
		IFF(ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('FnlToXfm') }}.CREATED_DATE IS NOT NULL, {{ ref('FnlToXfm') }}.CREATED_DATE, '')), '-'), TRIM(IFF({{ ref('FnlToXfm') }}.CREATED_DATE IS NOT NULL, {{ ref('FnlToXfm') }}.CREATED_DATE, ''))), '-'), TRIM(IFF({{ ref('FnlToXfm') }}.CREATED_DATE IS NOT NULL, {{ ref('FnlToXfm') }}.CREATED_DATE, '')))), 'Y', 'N') AS svIsValidCreatedDate,
		-- *SRC*: \(20)If IsNull(ToXfm.CHANNEL_CAT_ID) Then 'Y' Else  If ( IF IsNotNull((ToXfm.CHANNEL_CAT_ID)) THEN (ToXfm.CHANNEL_CAT_ID) ELSE "") = '' Then 'Y' Else  if Trim(ToXfm.CHANNEL_CAT_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('FnlToXfm') }}.CHANNEL_CAT_ID IS NULL, 'Y', IFF(IFF({{ ref('FnlToXfm') }}.CHANNEL_CAT_ID IS NOT NULL, {{ ref('FnlToXfm') }}.CHANNEL_CAT_ID, '') = '', 'Y', IFF(TRIM({{ ref('FnlToXfm') }}.CHANNEL_CAT_ID) = 0, 'Y', 'N'))) AS svIsNullChannelCatId,
		-- *SRC*: \(20)If IsNull(ToXfm.APPT_ORIG_C) Then 'Y' Else  If Trim(ToXfm.APPT_ORIG_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('FnlToXfm') }}.APPT_ORIG_C IS NULL, 'Y', IFF(TRIM({{ ref('FnlToXfm') }}.APPT_ORIG_C) = '', 'Y', 'N')) AS svIsNullApptOrigC,
		-- *SRC*: TRIM(ToXfm.APP_ID),
		TRIM({{ ref('FnlToXfm') }}.APP_ID) AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'Invalid Date' AS CONV_MAP_RULE_M,
		'APPT' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		-- *SRC*: TRIM(ToXfm.CREATED_DATE),
		TRIM({{ ref('FnlToXfm') }}.CREATED_DATE) AS VALU_CHNG_BFOR_X,
		'11/11/1111' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'APPT_CRAT_D' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_COM_BUS_APP' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSEPO" : Trim(ToXfm.APP_ID),
		CONCAT('CSEPO', TRIM({{ ref('FnlToXfm') }}.APP_ID)) AS TRSF_KEY_I
	FROM {{ ref('FnlToXfm') }}
	WHERE svIsValidCreatedDate = 'N'
)

SELECT * FROM Xfm__ToErrApptCratD