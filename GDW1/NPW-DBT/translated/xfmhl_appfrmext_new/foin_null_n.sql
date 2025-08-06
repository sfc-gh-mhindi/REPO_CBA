{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH FOIN_NULL_N AS (
	SELECT
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InXfmBusinessRules2.FOREIGN_INCOME_FLAG)) THEN (InXfmBusinessRules2.FOREIGN_INCOME_FLAG) ELSE 0)) = 0) Then 'N' else  if Trim(InXfmBusinessRules2.FOREIGN_INCOME_FLAG) = '' Then 'N' else  if num(InXfmBusinessRules2.FOREIGN_INCOME_FLAG) then ( if (StringToDecimal(TRIM(InXfmBusinessRules2.FOREIGN_INCOME_FLAG)) = 0) Then 'N' else InXfmBusinessRules2.FOREIGN_INCOME_FLAG) else  if TrimLeadingTrailing(InXfmBusinessRules2.FOREIGN_INCOME_FLAG) = 'N' Then 'N' else InXfmBusinessRules2.FOREIGN_INCOME_FLAG,
		IFF(
	    TRIM(IFF({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG IS NOT NULL, {{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG, 0)) = 0, 'N',     
	    IFF(
	        TRIM({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG) = '', 'N', 
	        IFF(NUM({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG), IFF(STRINGTODECIMAL(TRIM({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG)) = 0, 'N', {{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG), IFF(TRIMLEADINGTRAILING({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG) = 'N', 'N', {{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG))
	    )
	) AS SvFoinNullChk,
		-- *SRC*: 'CSE' : 'HL' : Trim(InXfmBusinessRules2.HL_APP_ID),
		CONCAT(CONCAT('CSE', 'HL'), TRIM({{ ref('CpyRename') }}.HL_APP_ID)) AS APPT_I,
		-- *SRC*: TrimLeadingTrailing(InXfmBusinessRules2.FOREIGN_INCOME_FLAG),
		TRIMLEADINGTRAILING({{ ref('CpyRename') }}.FOREIGN_INCOME_FLAG) AS APPT_ATTR_CLAS_C,
		-- *SRC*: TrimLeadingTrailing(InXfmBusinessRules2.FI_CURRENCY_CODE),
		TRIMLEADINGTRAILING({{ ref('CpyRename') }}.FI_CURRENCY_CODE) AS APPT_ATTR_CLAS_VALU_C,
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
	WHERE SvFoinNullChk = 'N'
)

SELECT * FROM FOIN_NULL_N