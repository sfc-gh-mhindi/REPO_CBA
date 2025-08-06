{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH Trans AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTran.SUBTYPE_CODE) Then 'Y' Else  If ( IF IsNotNull((ToTran.SUBTYPE_CODE)) THEN (ToTran.SUBTYPE_CODE) ELSE "") = '' Then 'Y' Else  If ToTran.SUBTYPE_CODE <> 'PO' Then 'Y' Else 'N',
		IFF({{ ref('Join') }}.SUBTYPE_CODE IS NULL, 'Y', IFF(IFF({{ ref('Join') }}.SUBTYPE_CODE IS NOT NULL, {{ ref('Join') }}.SUBTYPE_CODE, '') = '', 'Y', IFF({{ ref('Join') }}.SUBTYPE_CODE <> 'PO', 'Y', 'N'))) AS svIsNullSubtypeCode,
		-- *SRC*: \(20)If IsNull(ToTran.PO_OVERDRAFT_CAT_ID) Then 'Y' Else  If Trim(ToTran.PO_OVERDRAFT_CAT_ID) = '' Then 'Y' Else  If Trim(ToTran.PO_OVERDRAFT_CAT_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Join') }}.PO_OVERDRAFT_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('Join') }}.PO_OVERDRAFT_CAT_ID) = '', 'Y', IFF(TRIM({{ ref('Join') }}.PO_OVERDRAFT_CAT_ID) = 0, 'Y', 'N'))) AS svIsNullPoOverdraftCatId,
		APP_PROD_ID,
		APP_ID,
		-- *SRC*: StringToDecimal(ToTran.PDCT_N),
		STRINGTODECIMAL({{ ref('Join') }}.PDCT_N) AS PDCT_N,
		PO_OVERDRAFT_CAT_ID,
		svIsNullPoOverdraftCatId AS PO_OVERDRAFT_CAT_ID_CHK,
		svIsNullPoOverdraftCatId AS APPT_PDCT_CATG_C,
		svIsNullPoOverdraftCatId AS APPT_PDCT_DURT_C
	FROM {{ ref('Join') }}
	WHERE svIsNullSubtypeCode = 'N'
)

SELECT * FROM Trans