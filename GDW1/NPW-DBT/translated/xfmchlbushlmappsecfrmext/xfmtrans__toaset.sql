{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH XfmTrans__ToAset AS (
	SELECT
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.ASSET_LIABILITY_ID) Then 'N' Else  If Trim(InInChlBusHlmAppSec.ASSET_LIABILITY_ID) = '' Then 'N' Else 'Y',
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.ASSET_LIABILITY_ID IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.ASSET_LIABILITY_ID) = '', 'N', 'Y')) AS svAsetLbtyId,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.HLM_APP_ID) Then 'N' Else  If Trim(InInChlBusHlmAppSec.HLM_APP_ID) = '' Then 'N' Else 'Y',
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.HLM_APP_ID IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.HLM_APP_ID) = '', 'N', 'Y')) AS svHlmAppId,
		-- *SRC*: \(20)If svHlmAppId = 'Y' Then 'CSEHM' : Trim(InInChlBusHlmAppSec.HLM_APP_ID) Else '',
		IFF(svHlmAppId = 'Y', CONCAT('CSEHM', TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.HLM_APP_ID)), '') AS svApptI,
		-- *SRC*: \(20)If svAsetLbtyId = 'Y' Then 'CSEC1' : Trim(InInChlBusHlmAppSec.ASSET_LIABILITY_ID) Else '',
		IFF(svAsetLbtyId = 'Y', CONCAT('CSEC1', TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.ASSET_LIABILITY_ID)), '') AS svAssetI,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.TO_MS) Then 'N' Else  If Trim(InInChlBusHlmAppSec.TO_MS) = '' Then 'N' Else 'Y',
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.TO_MS IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.TO_MS) = '', 'N', 'Y')) AS svToMs,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.TO_BRANCH) Then 'N' Else  If Trim(InInChlBusHlmAppSec.TO_BRANCH) = '' Then 'N' Else 'Y',
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.TO_BRANCH IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.TO_BRANCH) = '', 'N', 'Y')) AS svToBrch,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.TO_AGENT) Then 'N' Else  If Trim(InInChlBusHlmAppSec.TO_AGENT) = '' Then 'N' Else 'Y',
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.TO_AGENT IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.TO_AGENT) = '', 'N', 'Y')) AS svToAgnt,
		svAssetI AS ASET_I,
		'UNKN' AS SECU_CODE_C,
		'N/A' AS SECU_CATG_C,
		-- *SRC*: Trim(InInChlBusHlmAppSec.ASSET_LIABILITY_ID),
		TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.ASSET_LIABILITY_ID) AS SRCE_SYST_ASET_I,
		'CSE' AS SRCE_SYST_C,
		'N/A' AS ASET_C,
		-- *SRC*: SetNull(),
		SETNULL() AS ORIG_SRCE_SYST_ASET_I,
		-- *SRC*: SetNull(),
		SETNULL() AS ORIG_SRCE_SYST_C,
		'N' AS ENVT_F,
		-- *SRC*: SetNull(),
		SETNULL() AS ASET_X,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'UNKN' AS ASET_LIBL_C,
		'UNKN' AS AL_CATG_C,
		'N' AS DUPL_ASET_F,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('SrcChlBusHlmAppSecDS') }}
	WHERE svAsetLbtyId = 'Y'
)

SELECT * FROM XfmTrans__ToAset