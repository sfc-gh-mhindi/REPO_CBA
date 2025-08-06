{{ config(materialized='view', tags=['XfmBusPrfBrkDataFrmExt']) }}

WITH Xfmbrkdata__ToUnidPatyGnrcPrfl AS (
	SELECT
		-- *SRC*: \(20)IF (IsNull(ToTrnsfm.GRDE_C) And IsNotNull(ToTrnsfm.GRADE)) THEN "N" ElSE "Y",
		IFF({{ ref('LookUpMT') }}.GRDE_C IS NULL AND {{ ref('LookUpMT') }}.GRADE IS NOT NULL, 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)IF (IsNull(ToTrnsfm.SUB_GRDE_C) And IsNotNull(ToTrnsfm.SUBGRADE)) THEN "N" ELSE "Y",
		IFF({{ ref('LookUpMT') }}.SUB_GRDE_C IS NULL AND {{ ref('LookUpMT') }}.SUBGRADE IS NOT NULL, 'N', 'Y') AS svlsValidRecord1,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.GRADE) THEN @TRUE ELSE @FALSE,
		IFF({{ ref('LookUpMT') }}.GRADE IS NULL, @TRUE, @FALSE) AS svGrdNulchk,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.SUBGRADE) THEN @TRUE ELSE @FALSE,
		IFF({{ ref('LookUpMT') }}.SUBGRADE IS NULL, @TRUE, @FALSE) AS svSbGrdNulchk,
		UNID_PATY_I,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: \(20)IF svGrdNulchk THEN SetNull() ELSE  IF IsNull(ToTrnsfm.GRDE_C) THEN '9999' ELSE ToTrnsfm.GRDE_C,
		IFF(svGrdNulchk, SETNULL(), IFF({{ ref('LookUpMT') }}.GRDE_C IS NULL, '9999', {{ ref('LookUpMT') }}.GRDE_C)) AS GRDE_C,
		-- *SRC*: \(20)IF svSbGrdNulchk THEN SetNull() ELSE  IF IsNull(ToTrnsfm.SUB_GRDE_C) THEN '9999' ELSE ToTrnsfm.SUB_GRDE_C,
		IFF(svSbGrdNulchk, SETNULL(), IFF({{ ref('LookUpMT') }}.SUB_GRDE_C IS NULL, '9999', {{ ref('LookUpMT') }}.SUB_GRDE_C)) AS SUB_GRDE_C,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.PRINT_ANYWHERE_FLAG) THEN SetNull() ELSE  IF Len(Trim(ToTrnsfm.PRINT_ANYWHERE_FLAG)) = 0 THEN SetNull() ELSE ToTrnsfm.PRINT_ANYWHERE_FLAG,
		IFF({{ ref('LookUpMT') }}.PRINT_ANYWHERE_FLAG IS NULL, SETNULL(), IFF(LEN(TRIM({{ ref('LookUpMT') }}.PRINT_ANYWHERE_FLAG)) = 0, SETNULL(), {{ ref('LookUpMT') }}.PRINT_ANYWHERE_FLAG)) AS PRNT_PRVG_F,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('LookUpMT') }}
	WHERE 
)

SELECT * FROM Xfmbrkdata__ToUnidPatyGnrcPrfl