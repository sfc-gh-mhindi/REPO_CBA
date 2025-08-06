{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH Xfm__ToErrRqm3 AS (
	SELECT
		-- *SRC*: \(20)IF (IsNull(ToTrnsfm.DOCU_DELY_RECV_C) And IsNotNull(ToTrnsfm.EXEC_DOCU_RECV_TYPE)) THEN "N" ElSE "Y",
		IFF({{ ref('LkpFileandMap') }}.DOCU_DELY_RECV_C IS NULL AND {{ ref('LkpFileandMap') }}.EXEC_DOCU_RECV_TYPE IS NOT NULL, 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.EXEC_DOCU_RECV_TYPE) THEN @TRUE ELSE @FALSE,
		IFF({{ ref('LkpFileandMap') }}.EXEC_DOCU_RECV_TYPE IS NULL, @TRUE, @FALSE) AS svlsValidRecord1,
		{{ ref('LkpFileandMap') }}.CHL_APP_HL_APP_ID AS SRCE_KEY_I,
		GDW_USER AS CONV_M,
		'Lookup failure on MAP_CSE_APPT_DOCU_DELYtable' AS CONV_MAP_RULE_M,
		'APPT_DOCU_DELY_INSS' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS SRCE_EFFT_D,
		{{ ref('LkpFileandMap') }}.EXEC_DOCU_RECV_TYPE AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DsjobName AS TRSF_X,
		'DOCU_DELY_RECV_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CHL_BUS_APP' AS SRCE_FILE_M,
		REFR_PK AS PROS_KEY_EFFT_I,
		{{ ref('LkpFileandMap') }}.HL_APP_ID AS TRSF_KEY_I
	FROM {{ ref('LkpFileandMap') }}
	WHERE svlsValidRecord = 'N'
)

SELECT * FROM Xfm__ToErrRqm3