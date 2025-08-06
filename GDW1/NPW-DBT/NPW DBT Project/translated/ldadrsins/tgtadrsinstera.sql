{{ config(materialized='incremental', alias='adrs', incremental_strategy='append', tags=['LdAdrsIns']) }}

SELECT
	ADRS_I
	ADRS_TYPE_C
	SRCE_SYST_C
	ADRS_QLFY_C
	SRCE_SYST_ADRS_I
	SRCE_SYST_ADRS_SEQN_N
	EFFT_D
	PROS_KEY_EFFT_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtAdrsInsertDS') }}