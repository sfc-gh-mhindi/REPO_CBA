{{ config(materialized='incremental', alias='util_pros_isac', incremental_strategy='append', tags=['UtilProcessMetaDataApptOrigTC']) }}

SELECT
	PROS_KEY_I
	SUCC_F
	COMT_F
	COMT_S
	MLTI_LOAD_EFFT_D
	SYST_S
	MLTI_LOAD_COMT_S
	SYST_ET_Q
	SYST_UV_Q
	SYST_INS_Q
	SYST_UPD_Q
	SYST_DEL_Q
	SYST_ET_TABL_M
	SYST_UV_TABL_M
	TRLR_RECD_ISRT_Q
	TRLR_RECD_UPDT_Q
	TRLR_RECD_DELT_Q 
FROM {{ ref('xf_CheckCounts__Ln_UpdateCtlTbl') }}