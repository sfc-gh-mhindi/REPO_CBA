{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH FOIN_Y AS (
	SELECT
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InXfmBusinessRules1.FI_CURRENCY_CODE)) THEN (InXfmBusinessRules1.FI_CURRENCY_CODE) ELSE 0)) = 0) Then 'N' else  if Trim(InXfmBusinessRules1.FI_CURRENCY_CODE) = '' Then 'N' else  if num(InXfmBusinessRules1.FI_CURRENCY_CODE) then ( if (StringToDecimal(TRIM(InXfmBusinessRules1.FI_CURRENCY_CODE)) = 0) Then 'N' else InXfmBusinessRules1.FI_CURRENCY_CODE) else InXfmBusinessRules1.FI_CURRENCY_CODE,
		IFF(
	    TRIM(IFF({{ ref('CpyRename') }}.FI_CURRENCY_CODE IS NOT NULL, {{ ref('CpyRename') }}.FI_CURRENCY_CODE, 0)) = 0, 'N', 
	    IFF(TRIM({{ ref('CpyRename') }}.FI_CURRENCY_CODE) = '', 'N', IFF(NUM({{ ref('CpyRename') }}.FI_CURRENCY_CODE), IFF(STRINGTODECIMAL(TRIM({{ ref('CpyRename') }}.FI_CURRENCY_CODE)) = 0, 'N', {{ ref('CpyRename') }}.FI_CURRENCY_CODE), {{ ref('CpyRename') }}.FI_CURRENCY_CODE))
	) AS SvFicncycdnullchkvalue,
		-- *SRC*: 'CSE' : 'HL' : Trim(InXfmBusinessRules1.HL_APP_ID),
		CONCAT(CONCAT('CSE', 'HL'), TRIM({{ ref('CpyRename') }}.HL_APP_ID)) AS APPT_I,
		-- *SRC*: TrimLeadingTrailing(InXfmBusinessRules1.FOREIGN_INCOME_FLAG),
		TRIMLEADINGTRAILING({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG) AS APPT_ATTR_CLAS_C,
		SvFicncycdnullchkvalue AS APPT_ATTR_CLAS_VALU_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: setNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('CpyRename') }}
	WHERE TRIMLEADINGTRAILING({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG) = 'Y' AND TRIMLEADINGTRAILING(SvFicncycdnullchkvalue) <> 'N'
)

SELECT * FROM FOIN_Y