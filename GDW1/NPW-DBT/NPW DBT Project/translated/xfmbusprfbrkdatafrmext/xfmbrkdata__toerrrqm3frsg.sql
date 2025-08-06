{{ config(materialized='view', tags=['XfmBusPrfBrkDataFrmExt']) }}

WITH Xfmbrkdata__ToErrRqm3frSg AS (
	SELECT
		-- *SRC*: \(20)IF (IsNull(ToTrnsfm.GRDE_C) And IsNotNull(ToTrnsfm.GRADE)) THEN "N" ElSE "Y",
		IFF({{ ref('LookUpMT') }}.GRDE_C IS NULL AND {{ ref('LookUpMT') }}.GRADE IS NOT NULL, 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)IF (IsNull(ToTrnsfm.SUB_GRDE_C) And IsNotNull(ToTrnsfm.SUBGRADE)) THEN "N" ELSE "Y",
		IFF({{ ref('LookUpMT') }}.SUB_GRDE_C IS NULL AND {{ ref('LookUpMT') }}.SUBGRADE IS NOT NULL, 'N', 'Y') AS svlsValidRecord1,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.GRADE) THEN @TRUE ELSE @FALSE,
		IFF({{ ref('LookUpMT') }}.GRADE IS NULL, @TRUE, @FALSE) AS svGrdNulchk,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.SUBGRADE) THEN @TRUE ELSE @FALSE,
		IFF({{ ref('LookUpMT') }}.SUBGRADE IS NULL, @TRUE, @FALSE) AS svSbGrdNulchk,
		{{ ref('LookUpMT') }}.ALIAS_ID AS SRCE_KEY_I,
		GDW_USER AS CONV_M,
		'Lookup failure on MAP_CSE_UNID_PATY_PRFL_SG table' AS CONV_MAP_RULE_M,
		'UNID_PATY_GNRC_PRFL' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.SUBGRADE) THEN SetNull() ELSE ToTrnsfm.SUBGRADE,
		IFF({{ ref('LookUpMT') }}.SUBGRADE IS NULL, SETNULL(), {{ ref('LookUpMT') }}.SUBGRADE) AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DsjobName AS TRSF_X,
		'SUB_GRDE_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_PRD_BUS_PRF_BRK_DATA' AS SRCE_FILE_M,
		REFR_PK AS PROS_KEY_EFFT_I,
		{{ ref('LookUpMT') }}.UNID_PATY_I AS TRSF_KEY_I
	FROM {{ ref('LookUpMT') }}
	WHERE svlsValidRecord1 = 'N'
)

SELECT * FROM Xfmbrkdata__ToErrRqm3frSg