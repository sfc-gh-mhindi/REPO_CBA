{{ config(materialized='view', tags=['UtilProcessMetaDataTC']) }}

WITH Update_Util_Pros__wtl AS (
	SELECT
		-- *SRC*: \(20)If ( IF IsNotNull((Update_Link.TRLR_RECD_ISRT_Q)) THEN (Update_Link.TRLR_RECD_ISRT_Q) ELSE 0) = ( IF IsNotNull((Update_Link.INS_CNT)) THEN (Update_Link.INS_CNT) ELSE 0) and ( IF IsNotNull((Update_Link.TRLR_RECD_UPDT_Q)) THEN (Update_Link.TRLR_RECD_UPDT_Q) ELSE 0) = ( IF IsNotNull((Update_Link.UPD_CNT)) THEN (Update_Link.UPD_CNT) ELSE 0) and ( IF IsNotNull((Update_Link.ERR_CNT1)) THEN (Update_Link.ERR_CNT1) ELSE 0) <= 0 and ( IF IsNotNull((Update_Link.ERR_CNT2)) THEN (Update_Link.ERR_CNT2) ELSE 0) <= 0 then 'Y' else 'N',
		IFF(    
	    IFF({{ ref('SYNC_TBL') }}.TRLR_RECD_ISRT_Q IS NOT NULL, {{ ref('SYNC_TBL') }}.TRLR_RECD_ISRT_Q, 0) = IFF({{ ref('SYNC_TBL') }}.INS_CNT IS NOT NULL, {{ ref('SYNC_TBL') }}.INS_CNT, 0)
	    and IFF({{ ref('SYNC_TBL') }}.TRLR_RECD_UPDT_Q IS NOT NULL, {{ ref('SYNC_TBL') }}.TRLR_RECD_UPDT_Q, 0) = IFF({{ ref('SYNC_TBL') }}.UPD_CNT IS NOT NULL, {{ ref('SYNC_TBL') }}.UPD_CNT, 0)
	    and IFF({{ ref('SYNC_TBL') }}.ERR_CNT1 IS NOT NULL, {{ ref('SYNC_TBL') }}.ERR_CNT1, 0) <= 0
	    and IFF({{ ref('SYNC_TBL') }}.ERR_CNT2 IS NOT NULL, {{ ref('SYNC_TBL') }}.ERR_CNT2, 0) <= 0, 
	    'Y', 
	    'N'
	) AS svSuccess,
		-- *SRC*: 'Job Failed in Reconciliation for Pros Key ' : Update_Link.PROS_KEY_I : '.' : ' {{ var(" of rows inserted in gdw target = 0' : ( if isnotnull((update_link.ins_cnt)) then (update_link.ins_cnt) else "") : '.  ") }} of rows in insert data file =  0' : ( IF IsNotNull((Update_Link.TRLR_RECD_ISRT_Q)) THEN (Update_Link.TRLR_RECD_ISRT_Q) ELSE "") : '.' : ' {{ var(" of rows updated in gdw target = 0' : ( if isnotnull((update_link.upd_cnt)) then (update_link.upd_cnt) else "") : '.  ") }} of rows in insert data file =  0' : ( IF IsNotNull((Update_Link.TRLR_RECD_UPDT_Q)) THEN (Update_Link.TRLR_RECD_UPDT_Q) ELSE "") : '.',
		CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('Job Failed in Reconciliation for Pros Key ', {{ ref('SYNC_TBL') }}.PROS_KEY_I), '.'), ' {{ var(" of rows inserted in gdw target = 0'), iff(sync_tbl.ins_cnt is not null, sync_tbl.ins_cnt, '')), '.  ") }} of rows in insert data file =  0'), IFF({{ ref('SYNC_TBL') }}.TRLR_RECD_ISRT_Q IS NOT NULL, {{ ref('SYNC_TBL') }}.TRLR_RECD_ISRT_Q, '')), '.'), ' {{ var(" of rows updated in gdw target = 0'), iff(sync_tbl.upd_cnt is not null, sync_tbl.upd_cnt, '')), '.  ") }} of rows in insert data file =  0'), IFF({{ ref('SYNC_TBL') }}.TRLR_RECD_UPDT_Q IS NOT NULL, {{ ref('SYNC_TBL') }}.TRLR_RECD_UPDT_Q, '')), '.') AS MessageToWrite,
		'Warning' AS MessageSeverity
	FROM {{ ref('SYNC_TBL') }}
	WHERE svSuccess = 'N'
)

SELECT * FROM Update_Util_Pros__wtl