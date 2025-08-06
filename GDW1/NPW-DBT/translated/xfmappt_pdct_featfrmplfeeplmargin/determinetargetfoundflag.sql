{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

WITH DetermineTargetFoundFlag AS (
	SELECT
		TGT_TBL_NAME,
		SRC_TBL_NAME,
		-- *SRC*: OutJoin.DLTD_KEY1_VALU[3, 10],
		SUBSTRING({{ ref('TMP_DELETED') }}.DLTD_KEY1_VALU, 3, 10) AS PL_FEE_ID,
		{{ ref('TMP_DELETED') }}.DLTD_KEY3_VALU AS PL_MARGIN_ID,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutJoin.APPT_PDCT_I)) THEN (OutJoin.APPT_PDCT_I) ELSE ""))) = 0 Then SetNull() Else Trim(OutJoin.APPT_PDCT_I[6, 250]),
		IFF(LEN(TRIM(IFF({{ ref('TMP_DELETED') }}.APPT_PDCT_I IS NOT NULL, {{ ref('TMP_DELETED') }}.APPT_PDCT_I, ''))) = 0, SETNULL(), TRIM(SUBSTRING({{ ref('TMP_DELETED') }}.APPT_PDCT_I, 6, 250))) AS APPT_PDCT_I,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutJoin.SRCE_SYST_APPT_FEAT_I)) THEN (OutJoin.SRCE_SYST_APPT_FEAT_I) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('TMP_DELETED') }}.SRCE_SYST_APPT_FEAT_I IS NOT NULL, {{ ref('TMP_DELETED') }}.SRCE_SYST_APPT_FEAT_I, ''))) = 0, 'N', 'Y') AS TGT_FOUND_FLAG
	FROM {{ ref('TMP_DELETED') }}
	WHERE 
)

SELECT * FROM DetermineTargetFoundFlag