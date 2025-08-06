{{ config(materialized='incremental', alias='phys_adrs', incremental_strategy='append', tags=['LdPhysAdrsIns']) }}

SELECT
	ADRS_I
	PHYS_ADRS_TYPE_C
	ADRS_LINE_1_X
	ADRS_LINE_2_X
	SURB_X
	CITY_X
	PCOD_C
	STAT_C
	ISO_CNTY_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtPhysAdrsInsertDS') }}