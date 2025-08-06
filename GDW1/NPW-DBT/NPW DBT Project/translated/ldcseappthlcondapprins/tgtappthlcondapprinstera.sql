{{ config(materialized='incremental', alias='appt_pdct_feat', incremental_strategy='append', tags=['LdCseApptHlCondApprIns']) }}

SELECT
	APPT_I
	EFFT_D
	COND_APPR_F
	COND_APPR_CONV_TO_FULL_D
	EXPY_D
	ROW_SECU_ACCS_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptHlCondApprInsertDS') }}