{{ config(materialized='view', tags=['LdTMP_APP_ANS_Ins']) }}

WITH Transformer_89 AS (
	SELECT
		-- *SRC*: \(20)If (IsNotNull(DSLink90.QSTN_C)) then  If Len(Trim(DSLink90.QSTN_C)) < 4 and Len(Trim(DSLink90.QSTN_C)) > 0 THEN Str('0', 4 - Len(DSLink90.QSTN_C)) : DSLink90.QSTN_C ELSE DSLink90.QSTN_C else '',
		IFF({{ ref('SrcApptDeptDS') }}.QSTN_C IS NOT NULL, IFF(LEN(TRIM({{ ref('SrcApptDeptDS') }}.QSTN_C)) < 4 AND LEN(TRIM({{ ref('SrcApptDeptDS') }}.QSTN_C)) > 0, CONCAT(STR('0', 4 - LEN({{ ref('SrcApptDeptDS') }}.QSTN_C)), {{ ref('SrcApptDeptDS') }}.QSTN_C), {{ ref('SrcApptDeptDS') }}.QSTN_C), '') AS SvQstnFrm,
		-- *SRC*: \(20)If (IsNotNull(DSLink90.RESP_C)) then  If Len(Trim(DSLink90.RESP_C)) < 4 and Len(Trim(DSLink90.RESP_C)) > 0 THEN Str('0', 4 - Len(DSLink90.RESP_C)) : DSLink90.RESP_C ELSE DSLink90.RESP_C else '',
		IFF({{ ref('SrcApptDeptDS') }}.RESP_C IS NOT NULL, IFF(LEN(TRIM({{ ref('SrcApptDeptDS') }}.RESP_C)) < 4 AND LEN(TRIM({{ ref('SrcApptDeptDS') }}.RESP_C)) > 0, CONCAT(STR('0', 4 - LEN({{ ref('SrcApptDeptDS') }}.RESP_C)), {{ ref('SrcApptDeptDS') }}.RESP_C), {{ ref('SrcApptDeptDS') }}.RESP_C), '') AS SvRespFrm,
		EVNT_I,
		-- *SRC*: \(20)if SvQstnFrm = '' then DSLink90.QSTN_C else SvQstnFrm,
		IFF(SvQstnFrm = '', {{ ref('SrcApptDeptDS') }}.QSTN_C, SvQstnFrm) AS QSTN_C,
		EFFT_D,
		-- *SRC*: \(20)if SvRespFrm = '' then DSLink90.RESP_C else SvRespFrm,
		IFF(SvRespFrm = '', {{ ref('SrcApptDeptDS') }}.RESP_C, SvRespFrm) AS RESP_C,
		RESP_CMMT_X,
		EXPY_D,
		PROS_KEY_EFFT_I,
		SRCE_SYST_EVNT_I,
		SRCE_SYST_C,
		EVNT_ACTV_TYPE_C_XS,
		EVNT_ACTV_TYPE_C,
		DEPT_ROLE_C,
		DEPT_I,
		EMPL_I,
		EVNT_PATY_ROLE_TYPE_C_EE,
		EVNT_PATY_ROLE_TYPE_C,
		SRCE_SYST_PATY_I,
		PATY_I,
		RELD_EVNT_I,
		EVNT_REL_TYPE_C,
		APPT_QLFY_C,
		-- *SRC*: StringToTimestamp((trim(DSLink90.MOD_TIMESTAMP)), '%yyyy%mm%dd%hh%nn%ss'),
		STRINGTOTIMESTAMP(TRIM({{ ref('SrcApptDeptDS') }}.MOD_TIMESTAMP), '%yyyy%mm%dd%hh%nn%ss') AS MOD_TIMESTAMP,
		RUN_STRM
	FROM {{ ref('SrcApptDeptDS') }}
	WHERE 
)

SELECT * FROM Transformer_89