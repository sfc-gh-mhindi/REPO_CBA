{{ config(materialized='view', tags=['UtilProcessMetaDataTC']) }}

WITH Update_Util_Pros__Update_Util_Proc_Table AS (
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
		PROS_KEY_I,
		svSuccess AS SUCC_F,
		'Y' AS COMT_F,
		DSJobStartTimestamp AS COMT_S,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd'),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS MLTI_LOAD_EFFT_D,
		-- *SRC*: \(20)if (Len(Update_Link.START_T) > 0) then Update_Link.START_T else '1111-11-11 00:00:00',
		IFF(LEN({{ ref('SYNC_TBL') }}.START_T) > 0, {{ ref('SYNC_TBL') }}.START_T, '1111-11-11 00:00:00') AS SYST_S,
		-- *SRC*: \(20)if (Len(Update_Link.END_T) > 0) then Update_Link.END_T else '1111-11-11 00:00:00',
		IFF(LEN({{ ref('SYNC_TBL') }}.END_T) > 0, {{ ref('SYNC_TBL') }}.END_T, '1111-11-11 00:00:00') AS MLTI_LOAD_COMT_S,
		{{ ref('SYNC_TBL') }}.ERR_CNT1 AS SYST_ET_Q,
		{{ ref('SYNC_TBL') }}.ERR_CNT2 AS SYST_UV_Q,
		{{ ref('SYNC_TBL') }}.INS_CNT AS SYST_INS_Q,
		{{ ref('SYNC_TBL') }}.UPD_CNT AS SYST_UPD_Q,
		{{ ref('SYNC_TBL') }}.DEL_CNT AS SYST_DEL_Q,
		-- *SRC*: \(20)If (Update_Link.ERR_CNT1 > 0) Then pGDW_TABLE_NAME : '_ET' Else SetNull(),
		IFF({{ ref('SYNC_TBL') }}.ERR_CNT1 > 0, CONCAT(pGDW_TABLE_NAME, '_ET'), SETNULL()) AS SYST_ET_TABL_M,
		-- *SRC*: \(20)If (Update_Link.ERR_CNT2 > 0) Then pGDW_TABLE_NAME : '_UT' Else SetNull(),
		IFF({{ ref('SYNC_TBL') }}.ERR_CNT2 > 0, CONCAT(pGDW_TABLE_NAME, '_UT'), SETNULL()) AS SYST_UV_TABL_M,
		TRLR_RECD_ISRT_Q,
		TRLR_RECD_UPDT_Q,
		TRLR_RECD_DELT_Q
	FROM {{ ref('SYNC_TBL') }}
	WHERE 
)

SELECT * FROM Update_Util_Pros__Update_Util_Proc_Table