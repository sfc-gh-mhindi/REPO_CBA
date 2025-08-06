{{ config(materialized='view', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

WITH Trans__InSubId AS (
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
		CONCAT(CONCAT('CSE', {{ ref('Lkp') }}.APPT_QLFY_C), {{ ref('Lkp') }}.APP_APP_ID) AS APPT_I,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd'),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		'0' AS ROW_SECU_ACCS_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: \(20)If StgOrigSubmit = 'Y' Then InTran.SUB_APPT_ORIG_C Else '9999',
		IFF(StgOrigSubmit = 'Y', {{ ref('Lkp') }}.SUB_APPT_ORIG_C, '9999') AS APPT_ORIG_C,
		'SUBM' AS APPT_ORIG_CATG_C
	FROM {{ ref('Lkp') }}
	WHERE StgApptQlfy = 'Y' AND StgSubIdConstraint = 'Y'
)

SELECT * FROM Trans__InSubId