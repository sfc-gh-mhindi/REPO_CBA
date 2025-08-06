{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT']) }}

WITH XFR_DeltaCheck__paty_Evn AS (
	SELECT
		-- *SRC*: \(20)If To_XFR_NullToEmpty.TGT_EVNT_I = "RE33" Then "L" Else "DL",
		IFF({{ ref('Src_Tera_XS_EVNT') }}.TGT_EVNT_I = 'RE33', 'L', 'DL') AS INSERT,
		-- *SRC*: \(20)If Len(Trim(To_XFR_NullToEmpty.CIF_CODE)) < 10 and Len(Trim(To_XFR_NullToEmpty.CIF_CODE)) > 0 THEN Str('0', 10 - Len(To_XFR_NullToEmpty.CIF_CODE)) : To_XFR_NullToEmpty.CIF_CODE ELSE To_XFR_NullToEmpty.CIF_CODE,
		IFF(LEN(TRIM({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE)) < 10 AND LEN(TRIM({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE)) > 0, CONCAT(STR('0', 10 - LEN({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE)), {{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE), {{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE) AS PATY,
		-- *SRC*: \(20)If Len(( IF IsNotNull((To_XFR_NullToEmpty.CIF_CODE)) THEN (To_XFR_NullToEmpty.CIF_CODE) ELSE "")) > '0' Then 'Y' Else 'N',
		IFF(LEN(IFF({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE IS NOT NULL, {{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE, '')) > '0', 'Y', 'N') AS PATYCOND,
		-- *SRC*: \(20)If To_XFR_NullToEmpty.APPT_QLFY_C = 'HL' Then '3134' Else '3135',
		IFF({{ ref('Src_Tera_XS_EVNT') }}.APPT_QLFY_C = 'HL', '3134', '3135') AS CODE,
		{{ ref('Src_Tera_XS_EVNT') }}.SRC_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE AS SRCE_SYST_PATY_I,
		'RESP' AS EVNT_PATY_ROLE_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: 'CIFPT+' : PATY,
		CONCAT('CIFPT+', PATY) AS PATY_I,
		'9999-12-31' AS EXPY_D,
		PATYEVNTPK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('Src_Tera_XS_EVNT') }}
	WHERE INSERT = 'L' AND PATYCOND = 'Y'
)

SELECT * FROM XFR_DeltaCheck__paty_Evn