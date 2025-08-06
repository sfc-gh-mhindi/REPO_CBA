{{ config(materialized='view', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

WITH Trans__ErrCtdId AS (
	SELECT
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InTran.APP_APP_ID)) THEN (InTran.APP_APP_ID) ELSE "")) = '' Or Trim(( IF IsNotNull((InTran.APP_APP_ID)) THEN (InTran.APP_APP_ID) ELSE ""), '0', 'L') = '') Then 'N' Else  If IsNull(InTran.APPT_QLFY_C) Then 'N' Else 'Y',
		IFF(TRIM(IFF({{ ref('Lkp') }}.APP_APP_ID IS NOT NULL, {{ ref('Lkp') }}.APP_APP_ID, '')) = '' OR TRIM(IFF({{ ref('Lkp') }}.APP_APP_ID IS NOT NULL, {{ ref('Lkp') }}.APP_APP_ID, ''), '0', 'L') = '', 'N', IFF({{ ref('Lkp') }}.APPT_QLFY_C IS NULL, 'N', 'Y')) AS StgApptQlfy,
		-- *SRC*: \(20)If IsNotNull(InTran.CHNL_APPT_ORIG_C) Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.CHNL_APPT_ORIG_C IS NOT NULL, 'Y', 'N') AS StgOrigChnl,
		-- *SRC*: \(20)If IsNotNull(InTran.CREATED_APPT_ORIG_C) Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.CREATED_APPT_ORIG_C IS NOT NULL, 'Y', 'N') AS StgOrigCreate,
		-- *SRC*: \(20)If IsNotNull(InTran.SUB_APPT_ORIG_C) Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.SUB_APPT_ORIG_C IS NOT NULL, 'Y', 'N') AS StgOrigSubmit,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InTran.APP_CHANNEL_CAT_ID)) THEN (InTran.APP_CHANNEL_CAT_ID) ELSE "")) = '' Or Trim(Trim(( IF IsNotNull((InTran.APP_CHANNEL_CAT_ID)) THEN (InTran.APP_CHANNEL_CAT_ID) ELSE "")), '0', 'L') = '') Then 'N' Else 'Y',
		IFF(TRIM(IFF({{ ref('Lkp') }}.APP_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_CHANNEL_CAT_ID, '')) = '' OR TRIM(TRIM(IFF({{ ref('Lkp') }}.APP_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_CHANNEL_CAT_ID, '')), '0', 'L') = '', 'N', 'Y') AS StgChnlIdConstraint,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InTran.APP_SUBMITTED_CHANNEL_CAT_ID)) THEN (InTran.APP_SUBMITTED_CHANNEL_CAT_ID) ELSE "")) = '' Or Trim(Trim(( IF IsNotNull((InTran.APP_SUBMITTED_CHANNEL_CAT_ID)) THEN (InTran.APP_SUBMITTED_CHANNEL_CAT_ID) ELSE "")), '0', 'L') = '') Then 'N' Else 'Y',
		IFF(TRIM(IFF({{ ref('Lkp') }}.APP_SUBMITTED_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_SUBMITTED_CHANNEL_CAT_ID, '')) = '' OR TRIM(TRIM(IFF({{ ref('Lkp') }}.APP_SUBMITTED_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_SUBMITTED_CHANNEL_CAT_ID, '')), '0', 'L') = '', 'N', 'Y') AS StgSubIdConstraint,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InTran.APP_CREATED_CHANNEL_CAT_ID)) THEN (InTran.APP_CREATED_CHANNEL_CAT_ID) ELSE "")) = '' Or Trim(Trim(( IF IsNotNull((InTran.APP_CREATED_CHANNEL_CAT_ID)) THEN (InTran.APP_CREATED_CHANNEL_CAT_ID) ELSE "")), '0', 'L') = '') Then 'N' Else 'Y',
		IFF(TRIM(IFF({{ ref('Lkp') }}.APP_CREATED_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_CREATED_CHANNEL_CAT_ID, '')) = '' OR TRIM(TRIM(IFF({{ ref('Lkp') }}.APP_CREATED_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_CREATED_CHANNEL_CAT_ID, '')), '0', 'L') = '', 'N', 'Y') AS StgCrtdIdConstraint,
		-- *SRC*: 'CSE' : InTran.APPT_QLFY_C : InTran.APP_APP_ID,
		CONCAT(CONCAT('CSE', {{ ref('Lkp') }}.APPT_QLFY_C), {{ ref('Lkp') }}.APP_APP_ID) AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'MAP_CSE_APPT_ORIG' AS CONV_MAP_RULE_M,
		'APPT_ORIG' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		-- *SRC*: Trim(( IF IsNotNull((InTran.APP_CREATED_CHANNEL_CAT_ID)) THEN (InTran.APP_CREATED_CHANNEL_CAT_ID) ELSE (''))),
		TRIM(IFF({{ ref('Lkp') }}.APP_CREATED_CHANNEL_CAT_ID IS NOT NULL, {{ ref('Lkp') }}.APP_CREATED_CHANNEL_CAT_ID, '')) AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'APPT_ORIG_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_COM_BUS_APP' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: 'CSE' : InTran.APPT_QLFY_C : InTran.APP_APP_ID : 'CRAT',
		CONCAT(CONCAT(CONCAT('CSE', {{ ref('Lkp') }}.APPT_QLFY_C), {{ ref('Lkp') }}.APP_APP_ID), 'CRAT') AS TRSF_KEY_I
	FROM {{ ref('Lkp') }}
	WHERE StgApptQlfy = 'Y' AND StgCrtdIdConstraint = 'Y' AND StgOrigCreate = 'N'
)

SELECT * FROM Trans__ErrCtdId