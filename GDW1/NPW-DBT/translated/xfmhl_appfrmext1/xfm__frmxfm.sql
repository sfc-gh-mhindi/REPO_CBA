{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH Xfm__FrmXfm AS (
	SELECT
		-- *SRC*: \(20)IF (IsNull(ToTrnsfm.DOCU_DELY_RECV_C) And IsNotNull(ToTrnsfm.EXEC_DOCU_RECV_TYPE)) THEN "N" ElSE "Y",
		IFF({{ ref('LkpFileandMap') }}.DOCU_DELY_RECV_C IS NULL AND {{ ref('LkpFileandMap') }}.EXEC_DOCU_RECV_TYPE IS NOT NULL, 'N', 'Y') AS svlsValidRecord,
		-- *SRC*: \(20)IF IsNull(ToTrnsfm.EXEC_DOCU_RECV_TYPE) THEN @TRUE ELSE @FALSE,
		IFF({{ ref('LkpFileandMap') }}.EXEC_DOCU_RECV_TYPE IS NULL, @TRUE, @FALSE) AS svlsValidRecord1,
		{{ ref('LkpFileandMap') }}.HL_APP_ID AS APPT_I,
		-- *SRC*: \(20)IF svlsValidRecord1 THEN SetNull() ELSE  IF IsNull(ToTrnsfm.DOCU_DELY_RECV_C) THEN '9999' ELSE ToTrnsfm.DOCU_DELY_RECV_C,
		IFF(svlsValidRecord1, SETNULL(), IFF({{ ref('LkpFileandMap') }}.DOCU_DELY_RECV_C IS NULL, '9999', {{ ref('LkpFileandMap') }}.DOCU_DELY_RECV_C)) AS DOCU_DELY_RECV_C,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('LkpFileandMap') }}
	WHERE 
)

SELECT * FROM Xfm__FrmXfm