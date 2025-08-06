{{ config(materialized='incremental', alias='tmp_appt_dept', incremental_strategy='append', tags=['LdTMP_PHYS_ADRSTrmXfm']) }}

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
	EFFT_D
	EXPY_D
	RUN_STRM
	CHL_PRCP_SCUY_FLAG 
FROM {{ ref('TgtTmp_Phys_AdrsDS') }}