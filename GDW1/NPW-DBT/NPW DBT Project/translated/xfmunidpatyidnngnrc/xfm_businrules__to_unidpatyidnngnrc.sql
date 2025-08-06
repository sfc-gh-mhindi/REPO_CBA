{{ config(materialized='view', tags=['XfmUnidPatyIdnnGnrc']) }}

WITH Xfm_BusinRules__To_UnidPatyIdnnGnrc AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.REFERRAL_AGENT_ID, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.REFERRAL_AGENT_ID) = '', 'N', IFF({{ ref('Modify_Null') }}.REFERRAL_AGENT_ID = '99999', 'N', 'Y'))) AS svRefAgntId,
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.HL_APP_ID, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.HL_APP_ID) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.HL_APP_ID = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.HL_APP_ID, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.HL_APP_ID) = '', 'N', IFF({{ ref('Modify_Null') }}.HL_APP_ID = '99999', 'N', 'Y'))) AS svHlAppId,
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.ABN, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.ABN) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.ABN = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.ABN, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.ABN) = '', 'N', IFF({{ ref('Modify_Null') }}.ABN = '99999', 'N', 'Y'))) AS svAbn,
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.ACN, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.ACN) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.ACN = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.ACN, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.ACN) = '', 'N', IFF({{ ref('Modify_Null') }}.ACN = '99999', 'N', 'Y'))) AS svAcn,
		'CSE' AS SRCE_SYST_C,
		'ABN' AS IDNN_TYPE_C,
		{{ ref('Modify_Null') }}.ABN AS IDNN_VALU_X,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		-- *SRC*: "CSEA3" : Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID,
		CONCAT('CSEA3', {{ ref('Modify_Null') }}.REFERRAL_AGENT_ID) AS UNID_PATY_I,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		'0' AS ROW_SECU_ACCS_C,
		pRUN_STRM_C AS RUN_STRM_C
	FROM {{ ref('Modify_Null') }}
	WHERE svHlAppId = 'Y' AND svAbn = 'Y' AND svRefAgntId = 'Y'
)

SELECT * FROM Xfm_BusinRules__To_UnidPatyIdnnGnrc