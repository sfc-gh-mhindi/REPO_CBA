{{ config(materialized='view', tags=['UtilProcessMetaDataApptOrigTC']) }}

WITH xf_CheckCounts__Ln_WriteWarning AS (
	SELECT
		-- *SRC*: \(20)If LnJoinedRecds.InsertCount = ( IF IsNotNull((LnJoinedRecds.INS_CNT)) THEN (LnJoinedRecds.INS_CNT) ELSE 0) and LnJoinedRecds.UpdateCount = ( IF IsNotNull((LnJoinedRecds.UPD_CNT)) THEN (LnJoinedRecds.UPD_CNT) ELSE 0) and LnJoinedRecds.DeleteCount = ( IF IsNotNull((LnJoinedRecds.DEL_CNT)) THEN (LnJoinedRecds.DEL_CNT) ELSE 0) and ( IF IsNotNull((LnJoinedRecds.ERR_CNT1)) THEN (LnJoinedRecds.ERR_CNT1) ELSE 0) <= 0 and ( IF IsNotNull((LnJoinedRecds.ERR_CNT2)) THEN (LnJoinedRecds.ERR_CNT2) ELSE 0) <= 0 Then 'Y' else 'N',
		IFF(    
	    {{ ref('jn_JoinSrceTrgt') }}.InsertCount = IFF({{ ref('jn_JoinSrceTrgt') }}.INS_CNT IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.INS_CNT, 0)
	    and {{ ref('jn_JoinSrceTrgt') }}.UpdateCount = IFF({{ ref('jn_JoinSrceTrgt') }}.UPD_CNT IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.UPD_CNT, 0)
	    and {{ ref('jn_JoinSrceTrgt') }}.DeleteCount = IFF({{ ref('jn_JoinSrceTrgt') }}.DEL_CNT IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.DEL_CNT, 0)
	    and IFF({{ ref('jn_JoinSrceTrgt') }}.ERR_CNT1 IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.ERR_CNT1, 0) <= 0
	    and IFF({{ ref('jn_JoinSrceTrgt') }}.ERR_CNT2 IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.ERR_CNT2, 0) <= 0, 
	    'Y', 
	    'N'
	) AS svSuccess,
		-- *SRC*: 'Job Failed in Reconciliation for Pros Key ' : LnJoinedRecds.SyncId : ', {{ var(" of records inserted in target = ' : lnjoinedrecds.ins_cnt : '  ") }} of records in insert  file = ' : LnJoinedRecds.InsertCount : ', {{ var(" of records updated in target = ' : lnjoinedrecds.upd_cnt : '  ") }} of records in update  file = ' : LnJoinedRecds.UpdateCount : ', {{ var(" of records deleted in target = ' : lnjoinedrecds.del_cnt : '  ") }} of records in delete file = ' : LnJoinedRecds.DeleteCount,
		CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('Job Failed in Reconciliation for Pros Key ', {{ ref('jn_JoinSrceTrgt') }}.SyncId), ', {{ var(" of records inserted in target = '), jn_joinsrcetrgt.ins_cnt), '  ") }} of records in insert  file = '), {{ ref('jn_JoinSrceTrgt') }}.InsertCount), ', {{ var(" of records updated in target = '), jn_joinsrcetrgt.upd_cnt), '  ") }} of records in update  file = '), {{ ref('jn_JoinSrceTrgt') }}.UpdateCount), ', {{ var(" of records deleted in target = '), jn_joinsrcetrgt.del_cnt), '  ") }} of records in delete file = '), {{ ref('jn_JoinSrceTrgt') }}.DeleteCount) AS MessageToWrite,
		'Warning' AS MessageSeverity
	FROM {{ ref('jn_JoinSrceTrgt') }}
	WHERE svSuccess = 'N'
)

SELECT * FROM xf_CheckCounts__Ln_WriteWarning