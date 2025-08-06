{{ config(materialized='view', tags=['XfmUnidPatyGnrc']) }}

WITH Xfm_BusinessRules AS (
	SELECT
		-- *SRC*: \(20)If (IsNull(Out_SrcCseChlBusAppComDet.HL_APP_ID) Or Len(Trim(Out_SrcCseChlBusAppComDet.HL_APP_ID, '0')) = 0 Or Trim(Out_SrcCseChlBusAppComDet.HL_APP_ID) = '') Then 'N' Else 'Y',
		IFF({{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID IS NULL OR LEN(TRIM({{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID, '0')) = 0 OR TRIM({{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID) = '', 'N', 'Y') AS AppVar,
		-- *SRC*: \(20)If (IsNull(Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID) Or Len(Trim(Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID, '0')) = 0 Or Trim(Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID) = '') Then 'N' Else 'Y',
		IFF({{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID IS NULL OR LEN(TRIM({{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID, '0')) = 0 OR TRIM({{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID) = '', 'N', 'Y') AS RefVar,
		-- *SRC*: 'CSEA3' : Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID,
		CONCAT('CSEA3', {{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID) AS UNID_PATY_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'U' AS PATY_TYPE_C,
		'TPRF' AS PATY_ROLE_C,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		'CSE' AS SRCE_SYST_C,
		'A3' AS PATY_QLFY_C,
		{{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID AS SRCE_SYST_PATY_I
	FROM {{ ref('SrcCseChlBusAppCommDet') }}
	WHERE AppVar = 'Y' AND RefVar = 'Y'
)

SELECT * FROM Xfm_BusinessRules