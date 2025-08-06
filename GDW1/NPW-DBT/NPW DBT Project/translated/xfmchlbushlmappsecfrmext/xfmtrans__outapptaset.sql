{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH XfmTrans__OutApptAset AS (
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
		HLM_APP_ID,
		ASSET_LIABILITY_ID,
		svApptI AS APPT_I,
		svAssetI AS ASET_I,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.PRINCIPAL_SECU_FLAG) Then SetNull() Else Trim(InInChlBusHlmAppSec.PRINCIPAL_SECU_FLAG),
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.PRINCIPAL_SECU_FLAG IS NULL, SETNULL(), TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.PRINCIPAL_SECU_FLAG)) AS PRIM_SECU_F,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS eror_seqn_i,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.SETTLEMENT_REQD) Then SetNull() Else  If Trim(InInChlBusHlmAppSec.SETTLEMENT_REQD) = '' Then SetNull() Else Trim(InInChlBusHlmAppSec.SETTLEMENT_REQD),
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.SETTLEMENT_REQD IS NULL, SETNULL(), IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.SETTLEMENT_REQD) = '', SETNULL(), TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.SETTLEMENT_REQD))) AS ASET_SETL_REQD,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.FORWARD_DOC_TO) Then 9999 Else  If Trim(InInChlBusHlmAppSec.FORWARD_DOC_TO) = '' Then 9999 Else InInChlBusHlmAppSec.FORWARD_DOC_TO,
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NULL, 9999, IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO) = '', 9999, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO)) AS FORWARD_DOC_TO,
		RUN_STREAM AS RUN_STRM,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: \(20)If ( IF IsNotNull((InInChlBusHlmAppSec.FORWARD_DOC_TO)) THEN (InInChlBusHlmAppSec.FORWARD_DOC_TO) ELSE "") = '1' And svToMs = 'Y' Then Trim(InInChlBusHlmAppSec.TO_MS) Else  If ( IF IsNotNull((InInChlBusHlmAppSec.FORWARD_DOC_TO)) THEN (InInChlBusHlmAppSec.FORWARD_DOC_TO) ELSE "") = '1' And svToMs = 'N' Then SetNull() Else  If ( IF IsNotNull((InInChlBusHlmAppSec.FORWARD_DOC_TO)) THEN (InInChlBusHlmAppSec.FORWARD_DOC_TO) ELSE "") = '2' And svToBrch = 'Y' Then Trim(InInChlBusHlmAppSec.TO_BRANCH) Else  If ( IF IsNotNull((InInChlBusHlmAppSec.FORWARD_DOC_TO)) THEN (InInChlBusHlmAppSec.FORWARD_DOC_TO) ELSE "") = '2' And svToBrch = 'N' Then SetNull() Else  If ( IF IsNotNull((InInChlBusHlmAppSec.FORWARD_DOC_TO)) THEN (InInChlBusHlmAppSec.FORWARD_DOC_TO) ELSE "") = '3' And svToAgnt = 'Y' Then Trim(InInChlBusHlmAppSec.TO_AGENT) Else  If ( IF IsNotNull((InInChlBusHlmAppSec.FORWARD_DOC_TO)) THEN (InInChlBusHlmAppSec.FORWARD_DOC_TO) ELSE "") = '3' And svToAgnt = 'N' Then SetNull() Else 'Unknown',
		IFF(
	    IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NOT NULL, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO, '') = '1' AND svToMs = 'Y', TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.TO_MS),     
	    IFF(
	        IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NOT NULL, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO, '') = '1'
	    and svToMs = 'N', SETNULL(),         
	        IFF(
	            IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NOT NULL, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO, '') = '2'
	        and svToBrch = 'Y', TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.TO_BRANCH),             
	            IFF(
	                IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NOT NULL, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO, '') = '2'
	            and svToBrch = 'N', SETNULL(),                 
	                IFF(
	                    IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NOT NULL, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO, '') = '3'
	                and svToAgnt = 'Y', TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.TO_AGENT), 
	                    IFF(IFF({{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO IS NOT NULL, {{ ref('SrcChlBusHlmAppSecDS') }}.FORWARD_DOC_TO, '') = '3'
	                    and svToAgnt = 'N', SETNULL(), 'Unknown')
	                )
	            )
	        )
	    )
	) AS SETL_LOCN_X,
		-- *SRC*: \(20)If IsNull(InInChlBusHlmAppSec.SETTLEMENT_COMMENT) Then SetNull() Else  If Trim(InInChlBusHlmAppSec.SETTLEMENT_COMMENT) = '' Then SetNull() Else Trim(InInChlBusHlmAppSec.SETTLEMENT_COMMENT),
		IFF({{ ref('SrcChlBusHlmAppSecDS') }}.SETTLEMENT_COMMENT IS NULL, SETNULL(), IFF(TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.SETTLEMENT_COMMENT) = '', SETNULL(), TRIM({{ ref('SrcChlBusHlmAppSecDS') }}.SETTLEMENT_COMMENT))) AS SETL_CMMT_X
	FROM {{ ref('SrcChlBusHlmAppSecDS') }}
	WHERE svHlmAppId = 'Y' AND svAsetLbtyId = 'Y'
)

SELECT * FROM XfmTrans__OutApptAset