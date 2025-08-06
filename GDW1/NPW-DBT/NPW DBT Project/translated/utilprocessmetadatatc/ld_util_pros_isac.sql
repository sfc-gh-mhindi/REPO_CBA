{{ config(materialized='incremental', alias='tdrnld.util_pros_isac', incremental_strategy='append', tags=['UtilProcessMetaDataTC']) }}

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
FROM {{ ref('Update_Util_Pros__Update_Util_Proc_Table') }}