{{ config(materialized='view', tags=['UtilProcessMetaDataApptOrigTC']) }}

WITH xf_CheckCounts__Ln_UpdateCtlTbl AS (
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
		{{ ref('jn_JoinSrceTrgt') }}.SyncId AS PROS_KEY_I,
		svSuccess AS SUCC_F,
		svSuccess AS COMT_F,
		-- *SRC*: \(20)If svSuccess = 'Y' Then DSJobStartTimestamp Else SetNull(),
		IFF(svSuccess = 'Y', DSJobStartTimestamp, SETNULL()) AS COMT_S,
		-- *SRC*: \(20)If svSuccess = 'Y' Then StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd') Else SetNull(),
		IFF(svSuccess = 'Y', STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd'), SETNULL()) AS MLTI_LOAD_EFFT_D,
		-- *SRC*: \(20)If svSuccess = 'Y' Then ( if IsNotNull(LnJoinedRecds.START_T) then LnJoinedRecds.START_T else '1111-11-11 00:00:00') Else SetNull(),
		IFF(svSuccess = 'Y', IFF({{ ref('jn_JoinSrceTrgt') }}.START_T IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.START_T, '1111-11-11 00:00:00'), SETNULL()) AS SYST_S,
		-- *SRC*: \(20)If svSuccess = 'Y' Then ( if IsNotNull(LnJoinedRecds.END_T) then LnJoinedRecds.END_T else '1111-11-11 00:00:00') Else SetNull(),
		IFF(svSuccess = 'Y', IFF({{ ref('jn_JoinSrceTrgt') }}.END_T IS NOT NULL, {{ ref('jn_JoinSrceTrgt') }}.END_T, '1111-11-11 00:00:00'), SETNULL()) AS MLTI_LOAD_COMT_S,
		{{ ref('jn_JoinSrceTrgt') }}.ERR_CNT1 AS SYST_ET_Q,
		{{ ref('jn_JoinSrceTrgt') }}.ERR_CNT2 AS SYST_UV_Q,
		{{ ref('jn_JoinSrceTrgt') }}.INS_CNT AS SYST_INS_Q,
		{{ ref('jn_JoinSrceTrgt') }}.UPD_CNT AS SYST_UPD_Q,
		{{ ref('jn_JoinSrceTrgt') }}.DEL_CNT AS SYST_DEL_Q,
		-- *SRC*: SetNull(),
		SETNULL() AS SYST_ET_TABL_M,
		-- *SRC*: SetNull(),
		SETNULL() AS SYST_UV_TABL_M,
		{{ ref('jn_JoinSrceTrgt') }}.InsertCount AS TRLR_RECD_ISRT_Q,
		{{ ref('jn_JoinSrceTrgt') }}.UpdateCount AS TRLR_RECD_UPDT_Q,
		{{ ref('jn_JoinSrceTrgt') }}.DeleteCount AS TRLR_RECD_DELT_Q
	FROM {{ ref('jn_JoinSrceTrgt') }}
	WHERE 
)

SELECT * FROM xf_CheckCounts__Ln_UpdateCtlTbl