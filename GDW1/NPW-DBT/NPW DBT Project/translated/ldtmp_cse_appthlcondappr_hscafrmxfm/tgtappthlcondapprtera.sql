{{ config(materialized='incremental', alias='tmp_appt_pdct_feat_pexa', incremental_strategy='append', tags=['LdTMP_CSE_APPTHLCONDAPPR_HSCAFrmXfm']) }}

SELECT
	APPT_I
	EFFT_D
	COND_APPR_F
	COND_APPR_CONV_TO_FULL_D
	EXPY_D
	ROW_SECU_ACCS_C 
FROM {{ ref('SrcApptHlCondApprDS') }}